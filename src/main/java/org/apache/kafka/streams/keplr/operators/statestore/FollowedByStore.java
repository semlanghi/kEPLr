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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.EXPIRED_WINDOW_RECORD_DROP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateAndCount;

/**
 * Byte-based implementation of the {@link FollowedByEventStore}. It uses three different structures to store the events.
 * Events are stored as "interval events", i.e. events with a duration. Each event is defined by their {@link IInterval},
 * and their key, here represented in {@link Bytes}.
 * The {@link IntervalTree} structure is used to keep all intervals for a certain key, and for retrieving the
 * overlapping intervals with a given one.
 * We also keep a structure to maintain the last iteration of for a specific event.
 *
 * @see ConcurrentNavigableMap
 * @see IntervalTree
 * @see IInterval
 */

public class FollowedByStore extends InMemoryWindowStore implements FollowedByEventStore<Bytes,byte[]> {




    private static final int SEQNUM_SIZE = 4;

    private final String name;

    private final String metricScope;
    private InternalProcessorContext context;
    private Sensor expiredRecordSensor;

    private final ConcurrentNavigableMap<Bytes, IntervalTree> comparationBase = new ConcurrentSkipListMap<>();
    private final ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<IInterval<Long>, byte[]>> immutableEvents = new ConcurrentSkipListMap<>();
    private final ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<IInterval<Long>, Long>> lastIterationEvents = new ConcurrentSkipListMap<>();

    private static Logger LOGGER = LoggerFactory.getLogger(FollowedByStore.class);


    private final Set<FollowedByWindowStoreIteratorWrapper<?>> openIterators = ConcurrentHashMap.newKeySet();
    private final Long withinMs;

    public FollowedByStore(String name, String metricScope, long retentionPeriod, long windowSize, boolean retainDuplicates, long numberPreds, long numberSucc, Long withinMs) {
        super(name, retentionPeriod, windowSize, retainDuplicates, metricScope);
        this.name = name;
        this.metricScope = metricScope;

        this.withinMs = withinMs;

    }

    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        this.context = (InternalProcessorContext) context;

        final StreamsMetricsImpl metrics = this.context.metrics();
        final String taskName = context.taskId().toString();
        expiredRecordSensor = metrics.storeLevelSensor(
                taskName,
                name(),
                EXPIRED_WINDOW_RECORD_DROP,
                Sensor.RecordingLevel.INFO
        );
        addInvocationRateAndCount(
                expiredRecordSensor,
                "stream-" + metricScope + "-metrics",
                metrics.tagMap("task-id", taskName, metricScope + "-id", name()),
                EXPIRED_WINDOW_RECORD_DROP
        );

        if (root != null) {
            context.register(root, (key, value) -> {
                put(Bytes.wrap(extractStoreKeyBytes(key)), value, extractStoreTimestamp(key));
            });
        }
        boolean open = true;
    }

    private static final int TIMESTAMP_SIZE = 8;

    static byte[] extractStoreKeyBytes(final byte[] binaryKey) {
        final byte[] bytes = new byte[binaryKey.length - TIMESTAMP_SIZE - SEQNUM_SIZE];
        System.arraycopy(binaryKey, 0, bytes, 0, bytes.length);
        return bytes;
    }

    static long extractStoreTimestamp(final byte[] binaryKey) {
        return ByteBuffer.wrap(binaryKey).getLong(binaryKey.length - TIMESTAMP_SIZE - SEQNUM_SIZE);
    }

    @Override
    public void put(Bytes key, byte[] value) {
        put(key, value, context.timestamp());
    }

    @Override
    public void put(Bytes key, byte[] value, long timestamp) {

        putIntervalEvent(key, value, timestamp,timestamp, true);
    }

    private static org.apache.kafka.common.utils.Bytes wrapForDups(final org.apache.kafka.common.utils.Bytes key, final int seqnum) {
        final ByteBuffer buf = ByteBuffer.allocate(key.get().length + SEQNUM_SIZE);
        buf.put(key.get());
        buf.putInt(seqnum);

        return org.apache.kafka.common.utils.Bytes.wrap(buf.array());
    }


    @Override
    public byte[] fetch(Bytes key, long windowStartTimestamp) {
        return null;
    }



    @Override
    public WindowStoreIterator<byte[]> fetch(Bytes key, long timeFrom, long timeTo) {
        return null;
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from, Bytes to, long timeFrom, long timeTo) {
        return null;
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
        return null;
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(long timeFrom, long timeTo) {
        return null;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {

    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return false;
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



    private ConcurrentSkipListSet<IInterval> toDelete= new ConcurrentSkipListSet();
    @Override
    public KeyValueIterator<Bytes, byte[]> fetchEventsInLeft(Bytes key, long start, long end, boolean delete) {

        IInterval<Long> interval = new LongInterval(start,end);
        IInterval<Long> searchInterval = new LongInterval(end-withinMs,start);

        if(lastCompositeGarbaging+withinMs*10<end){
            garbageCollectorComposite(key,end);
            lastCompositeGarbaging = end;
        }

        lastIterationEvents.computeIfAbsent(key, bytes -> new ConcurrentSkipListMap<>());
        immutableEvents.computeIfAbsent(key, bytes -> new ConcurrentSkipListMap<>((o1, o2) -> o1.getNormEnd().compareTo(o2.getNormEnd())));
        ConcurrentNavigableMap<IInterval<Long>, byte[]> map = immutableEvents.get(key).headMap(interval);
        comparationBase.computeIfAbsent(key, bytes -> IntervalTreeBuilder.newBuilder()
                .usePredefinedType(IntervalTreeBuilder.IntervalType.LONG)
                .collectIntervals(interval1 -> new ListIntervalCollection())
                .build());

        Iterator<Map.Entry<Bytes, byte[]>> it = comparationBase.get(key).overlapStream(searchInterval).filter(interval13 -> {
            Long temp = lastIterationEvents.get(key).get(interval13);
            if(map.containsKey(interval13) && start>temp){
                lastIterationEvents.get(key).put(interval13, end);
                if(delete){
                    toDelete.add(interval13);
                }
                return true;
            }else{
                return false;
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
        Iterator<Map.Entry<Bytes, byte[]>> it = comparationBase.get(key).overlapStream(interval1).filter(new Predicate<IInterval>() {
            @Override
            public boolean test(IInterval interval) {
                return ((NumberInterval)interval1).irIncludes(interval);
            }
        }).map((Function<IInterval, Map.Entry<Bytes, byte[]>>) interval -> new HashMap.SimpleEntry<>(key,immutableEvents.get(key).get(interval))).iterator();


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

        private Iterator<? extends Map.Entry<Bytes, V>> recordIterator;

        private Map.Entry<Bytes, V> next;


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
