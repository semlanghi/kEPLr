package org.apache.kafka.streams.keplr.operators.statestore;

import com.brein.time.timeintervals.collections.ListIntervalCollection;
import com.brein.time.timeintervals.indexes.IntervalTree;
import com.brein.time.timeintervals.indexes.IntervalTreeBuilder;
import com.brein.time.timeintervals.intervals.IInterval;
import com.brein.time.timeintervals.intervals.LongInterval;
import com.brein.time.timeintervals.intervals.NumberInterval;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.InMemoryWindowStore;


import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.EXPIRED_WINDOW_RECORD_DROP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateAndCount;


public class FollowedByStore extends InMemoryWindowStore implements FollowedByEventStore<Bytes,byte[]> {




    private static final int SEQNUM_SIZE = 4;

    private final String name;

    private final String metricScope;
    private InternalProcessorContext context;
    private Sensor expiredRecordSensor;

    private final ConcurrentNavigableMap<Bytes, IntervalTree> comparationBase = new ConcurrentSkipListMap<>();

    private final ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<IInterval<Long>, byte[]>> immutableEvents = new ConcurrentSkipListMap<>();


    private final ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<IInterval<Long>, Long>> lastIterationEvents = new ConcurrentSkipListMap<>();


    private final Set<FollowedByWindowStoreIteratorWrapper<?>> openIterators = ConcurrentHashMap.newKeySet();
    private final Long withinMs;

    public FollowedByStore(String name, String metricScope, long retentionPeriod, long windowSize, boolean retainDuplicates, long numberPreds, long numberSucc, Long withinMs) {
        super(name, retentionPeriod, windowSize, retainDuplicates, metricScope);
        this.name = name;
        this.metricScope = metricScope;

        this.withinMs = withinMs;

    }




    @Override
    public void putIntervalEvent(Bytes key, byte[] value, long start, long end, boolean allowOverlaps) {
        IInterval<Long> interval = new LongInterval(start, end);


        lastIterationEvents.computeIfAbsent(key, bytes -> new ConcurrentSkipListMap<>());


        immutableEvents.computeIfAbsent(key, bytes -> new ConcurrentSkipListMap<>(new Comparator<IInterval<Long>>() {
            @Override
            public int compare(IInterval<Long> o1, IInterval<Long> o2) {
                return o1.getNormEnd().compareTo(o2.getNormEnd());
            }
        }));

        //ArrayList<IInterval> toRemove = new ArrayList<>();




        comparationBase.computeIfAbsent(key, bytes -> IntervalTreeBuilder.newBuilder()
                .usePredefinedType(IntervalTreeBuilder.IntervalType.LONG)
                .collectIntervals(interval1 -> new ListIntervalCollection())
                .build());
        if(!allowOverlaps){
            if(comparationBase.get(key).overlapStream(interval).findAny().isPresent())
                return;
        }

        comparationBase.get(key).add(interval);
        immutableEvents.get(key).put(interval,value);
        lastIterationEvents.get(key).put(interval, interval.getNormEnd());
    }

    long lastCompositeGarbaging=0;


    public void garbageCollectorComposite(Bytes key, long end){

        if(comparationBase.get(key)!=null) {

            Collection<IInterval> reinsert= comparationBase.get(key).overlap(new LongInterval(end-withinMs, end));

            comparationBase.get(key).clear();
            comparationBase.get(key).addAll(reinsert);
            lastIterationEvents.get(key).headMap(new LongInterval(end-withinMs, end-withinMs)).clear();
            immutableEvents.get(key).headMap(new LongInterval(end-withinMs, end-withinMs)).clear();

        }
    }



    private ArrayList<IInterval> toDelete= new ArrayList<>();
    @Override
    public KeyValueIterator<Bytes, byte[]> fetchEventsInLeft(Bytes key, long start, long end, boolean delete) {
        IInterval<Long> interval = new LongInterval(start,end);
        IInterval<Long> searchInterval = new LongInterval(end-withinMs,start);

        if(lastCompositeGarbaging+withinMs*10<end){
            garbageCollectorComposite(key,end);
            lastCompositeGarbaging = end;
        }

        lastIterationEvents.computeIfAbsent(key, bytes -> new ConcurrentSkipListMap<>());
        immutableEvents.computeIfAbsent(key, bytes -> new ConcurrentSkipListMap<>(new Comparator<IInterval<Long>>() {
            @Override
            public int compare(IInterval<Long> o1, IInterval<Long> o2) {
                return o1.getNormEnd().compareTo(o2.getNormEnd());
            }
        }));
        ConcurrentNavigableMap<IInterval<Long>, byte[]> map = immutableEvents.get(key).headMap(interval);
        comparationBase.computeIfAbsent(key, bytes -> IntervalTreeBuilder.newBuilder()
                .usePredefinedType(IntervalTreeBuilder.IntervalType.LONG)
                .collectIntervals(interval1 -> new ListIntervalCollection())
                .build());

        Iterator<Map.Entry<Bytes, byte[]>> it = comparationBase.get(key).overlapStream(searchInterval).filter(new Predicate<IInterval>() {
            @Override
            public boolean test(IInterval interval) {
                Long temp = lastIterationEvents.get(key).get(interval);
                if(map.containsKey(interval) && start>temp){
                    lastIterationEvents.get(key).put(interval, end);
                    if(delete){
                        toDelete.add(interval);
                    }
                    return true;
                }else{
                    return false;
                }
            }
        }).sorted((o1, o2) -> {
            if(o1.getNormEnd().compareTo(o2.getNormEnd())==0){
                return o1.getNormStart().compareTo(o2.getNormStart());
            }else return  o1.getNormEnd().compareTo(o2.getNormEnd());
        }).map((Function<IInterval, Map.Entry<Bytes, byte[]>>) interval12 -> new HashMap.SimpleEntry<>(key,immutableEvents.get(key).get(interval12))).iterator();

        toDelete.forEach(t -> {
            lastIterationEvents.get(key).remove(t);
            comparationBase.get(key).remove(t);
            immutableEvents.get(key).remove(t);
        });

        toDelete.clear();

        return new FollowedByWindowStoreIteratorWrapper<>(it,openIterators::remove, false);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> fetchEventsInRight(Bytes key, long start, long end) {
        IInterval<Long> interval1 = new LongInterval(start,end);

        comparationBase.computeIfAbsent(key, bytes -> IntervalTreeBuilder.newBuilder()
                .usePredefinedType(IntervalTreeBuilder.IntervalType.LONG)
                .collectIntervals(interval11 -> new ListIntervalCollection())
                .build());
        Iterator<Map.Entry<Bytes, byte[]>> it = comparationBase.get(key).overlapStream(interval1).filter(interval -> ((NumberInterval)interval1).irIncludes(interval)).map((Function<IInterval, Map.Entry<Bytes, byte[]>>) interval -> new HashMap.SimpleEntry<>(key,immutableEvents.get(key).get(interval))).iterator();

        return new FollowedByWindowStoreIteratorWrapper<>(it,openIterators::remove, false);

    }


    interface ClosingCallback {
        void deregisterIterator(final FollowedByStore.FollowedByWindowStoreIteratorWrapper iterator);
    }
    private static org.apache.kafka.common.utils.Bytes getKey(final org.apache.kafka.common.utils.Bytes keyBytes) {
        final byte[] bytes = new byte[keyBytes.get().length  - SEQNUM_SIZE];
        System.arraycopy(keyBytes.get(), 0, bytes, 0, bytes.length);
        return org.apache.kafka.common.utils.Bytes.wrap(bytes);

    }

    private static class FollowedByWindowStoreIteratorWrapper<V>  implements KeyValueIterator<Bytes,V>{

        private Iterator<? extends Map.Entry<Long, V>> recordIterator2;
        private Iterator<? extends Map.Entry<Bytes, V>> recordIterator;

        private Map.Entry<Bytes, V> next;
        private long currentTime;

        private final boolean retainDuplicates;
        private final FollowedByStore.ClosingCallback callback;

        FollowedByWindowStoreIteratorWrapper(final Iterator<Map.Entry<Bytes, V>> recordIterator,
                                           final FollowedByStore.ClosingCallback callback,
                                           final boolean retainDuplicates) {
            this.retainDuplicates = retainDuplicates;
            this.recordIterator = recordIterator;
            this.callback = callback;
        }

        public boolean hasNext() {
            return recordIterator.hasNext();
        }

        @Override
        public KeyValue<Bytes, V> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            next = recordIterator.next();
            final KeyValue<Bytes, V> result = new KeyValue<>(next.getKey(), next.getValue());

            return result;
        }

        public void close() {
            recordIterator = null;
            callback.deregisterIterator(this);
        }

        @Override
        public Bytes peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            System.out.println("Unsupported operation.");
            return null;
        }

        protected KeyValue<Bytes, V> getNext() {
            System.out.println("Unsupported operation.");
            return null;
        }

    }




}
