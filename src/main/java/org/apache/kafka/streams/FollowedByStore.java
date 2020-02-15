package org.apache.kafka.streams;

import com.brein.time.timeintervals.collections.IntervalCollection;
import com.brein.time.timeintervals.collections.IntervalCollectionFactory;
import com.brein.time.timeintervals.collections.ListIntervalCollection;
import com.brein.time.timeintervals.indexes.IntervalTree;
import com.brein.time.timeintervals.indexes.IntervalTreeBuilder;
import com.brein.time.timeintervals.intervals.IInterval;
import com.brein.time.timeintervals.intervals.LongInterval;
import com.brein.time.timeintervals.intervals.NumberInterval;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStoreIterator;


import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.EXPIRED_WINDOW_RECORD_DROP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateAndCount;


public class FollowedByStore implements FollowedByEventStore<Bytes,byte[]> {


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
        this.name = name;
        this.metricScope = metricScope;

        this.withinMs = withinMs;

    }

    private static long counter = 0;

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
        //counter++;
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

    private void removeExpiredSegments() {
        //long minLiveTime = Math.max(0L, observedStreamTime - retentionPeriod + 1);
//        for (final FollowedByWindowStore.FollowedByWindowStoreIteratorWrapper it : openIterators) {
//            minLiveTime = Math.min(minLiveTime, context.timestamp());
//        }
        //WindowValueMap.headMap(minLiveTime, false).clear();
    }


    @Override
    public byte[] fetch(Bytes key, long windowStartTimestamp) {


        /*Objects.requireNonNull(key, "key cannot be null");

        removeExpiredSegments();

        if (windowStartTimestamp <= observedStreamTime - retentionPeriod) {
            return null;
        }

        final ConcurrentSkipListSet<Long> wvMap = this.KeyWindowMap.get(key);
        if (wvMap == null) {
            return null;
        } else {

            List<byte[]> collection = this.tree.overlapStream(new LongInterval(windowStartTimestamp,windowStartTimestamp))
                    .map(interval -> interval.getNormStart())
                    .sorted()
                    .map(start -> {
                        WindowSuccMap.put((Long) start,WindowSuccMap.get(start) -1);
                        return WindowValueMap.get(start);
                    })
                    .collect(Collectors.toList());


            return collection.get(0);

        }*/
        return null;
    }



    @Override
    public WindowStoreIterator<byte[]> fetch(Bytes key, long timeFrom, long timeTo) {

        /*
        Objects.requireNonNull(key, "key cannot be null");

        removeExpiredSegments();

        if (timeFrom <= observedStreamTime - retentionPeriod) {
            return null;
        }

        final ConcurrentSkipListSet<Long> wvMap = this.KeyWindowMap.get(key);
        if (wvMap == null) {
            return null;
        } else {
            //overlapStream only iff timeFrom==timeTo
            ConcurrentNavigableMap<Long,byte[]> collection = this.tree.overlapStream(new LongInterval(timeFrom,timeTo))
                    .map(new Function<IInterval, Long>() {
                        @Override
                        public Long apply(IInterval interval) {
                            Long start = (Long)interval.getNormStart();
                            WindowSuccMap.put(start,WindowSuccMap.get(start) -1);
                            return start;
                        }
                    })
                    .reduce(new ConcurrentSkipListMap<Long, byte[]>(),
                            (map,time) -> {map.put(time,WindowValueMap.get(time));
                                            return map;},
                            (map1,map2) -> {map1.putAll(map2);
                                           return map1;});



            return null;//new FollowedByWindowStoreIteratorWrapper(collection.entrySet().iterator(), openIterators::remove, false);

        }

         */
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



    int i=0;

    private void garbageCollection(){
        /*if(streamTime<context.timestamp())
            streamTime = context.timestamp();

        compositeEvents.keySet()
                .forEach(new Consumer<Bytes>() {
                    @Override
                    public void accept(Bytes bytes) {
                        compositeEvents.get(bytes).headMap(new LongInterval(streamTime-retentionPeriod, streamTime-retentionPeriod), true).clear();
                    }
                });

        singleEvents.keySet().forEach(new Consumer<Bytes>() {
            @Override
            public void accept(Bytes bytes) {
                singleEvents.get(bytes).headMap(streamTime-retentionPeriod).clear();
            }
        });*/
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
                return;/*stream().map(new Function<IInterval, NumberInterval>() {
                @Override
                public NumberInterval apply(IInterval interval) {
                    //System.out.println(interval.toString());
                    return new LongInterval((Long) interval.getNormEnd(),
                            (interval.getNormEnd().equals(lastIterationEvents.get(interval)) ? (Long) interval.getNormEnd()+withinMs : lastIterationEvents.get(interval)) );
                }
            }).anyMatch(new Predicate<NumberInterval>() {
                @Override
                public boolean test(NumberInterval interval1) {
                    return interval1.overlaps(interval);
                }
            })) return;*/
        }

        comparationBase.get(key).add(interval);
        immutableEvents.get(key).put(interval,value);
        lastIterationEvents.get(key).put(interval, interval.getNormEnd());
    }

    long lastCompositeGarbaging=0;

    Set<IInterval> toRemove1 = new HashSet<>();

    public void garbageCollectorComposite(Bytes key, long end){

        if(comparationBase.get(key)!=null) {
            /*comparationBase.get(key).forEach(new Consumer<IInterval>() {
                @Override
                public void accept(IInterval interval) {
                    if (((Long) interval.getNormEnd()) + withinMs < end) {
                        lastIterationEvents.get(key).remove(interval);
                        //comparationBase.get(key).remove(interval);
                        toRemove1.add(interval);
                        immutableEvents.get(key).remove(interval);
                    }
                }
            });*/

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

        //ArrayList<IInterval<Long>> toDelete

        lastIterationEvents.computeIfAbsent(key, bytes -> new ConcurrentSkipListMap<>());
        immutableEvents.computeIfAbsent(key, bytes -> new ConcurrentSkipListMap<>(new Comparator<IInterval<Long>>() {
            @Override
            public int compare(IInterval<Long> o1, IInterval<Long> o2) {
                return o1.getNormEnd().compareTo(o2.getNormEnd());
            }
        }));
        ConcurrentNavigableMap<IInterval<Long>, byte[]> map = immutableEvents.get(key).headMap(interval);
        comparationBase.computeIfAbsent(key, new Function<Bytes, IntervalTree>() {
            @Override
            public IntervalTree apply(Bytes bytes) {
                return IntervalTreeBuilder.newBuilder()
                        .usePredefinedType(IntervalTreeBuilder.IntervalType.LONG)
                        .collectIntervals(interval1 -> new ListIntervalCollection())
                        .build();
            }
        });

        Iterator<Map.Entry<Bytes, byte[]>> it = comparationBase.get(key).overlapStream(searchInterval).filter(new Predicate<IInterval>() {
            @Override
            public boolean test(IInterval interval) {
                //System.out.println("testing "+interval+map.containsKey(interval)+"\n");

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
        }).sorted(new Comparator<IInterval>() {
            @Override
            public int compare(IInterval o1, IInterval o2) {
                if(o1.getNormEnd().compareTo(o2.getNormEnd())==0){
                    return o1.getNormStart().compareTo(o2.getNormStart());
                }else return  o1.getNormEnd().compareTo(o2.getNormEnd());
            }
        }).map(new Function<IInterval, Map.Entry<Bytes,byte[]>>() {


            @Override
            public Map.Entry<Bytes, byte[]> apply(IInterval interval) {
                /*if(delete) {
                    //lastIterationEvents.remove(interval);

                    toDelete.add(interval);
                    toDelete.
                    //comparationBase.remove(interval);

                }

                 */
                return new HashMap.SimpleEntry<>(key,immutableEvents.get(key).get(interval));

            }
        }).iterator();

        //System.out.println(toDelete);
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

        comparationBase.computeIfAbsent(key, new Function<Bytes, IntervalTree>() {
            @Override
            public IntervalTree apply(Bytes bytes) {
                return IntervalTreeBuilder.newBuilder()
                        .usePredefinedType(IntervalTreeBuilder.IntervalType.LONG)
                        .collectIntervals(interval1 -> new ListIntervalCollection())
                        .build();
            }
        });
        Iterator<Map.Entry<Bytes, byte[]>> it = comparationBase.get(key).overlapStream(interval1).filter(new Predicate<IInterval>() {
            @Override
            public boolean test(IInterval interval) {
                return ((NumberInterval)interval1).irIncludes(interval);
            }
        }).map(new Function<IInterval, Map.Entry<Bytes,byte[]>>() {
            @Override
            public Map.Entry<Bytes, byte[]> apply(IInterval interval) {
                return new HashMap.SimpleEntry<>(key,immutableEvents.get(key).get(interval));
            }
        }).iterator();

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
