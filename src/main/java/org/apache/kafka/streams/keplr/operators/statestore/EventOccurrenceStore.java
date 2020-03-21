package org.apache.kafka.streams.keplr.operators.statestore;


import com.brein.time.timeintervals.collections.ListIntervalCollection;
import com.brein.time.timeintervals.indexes.IntervalTree;
import com.brein.time.timeintervals.indexes.IntervalTreeBuilder;
import com.brein.time.timeintervals.intervals.IInterval;
import com.brein.time.timeintervals.intervals.LongInterval;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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


import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.EXPIRED_WINDOW_RECORD_DROP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateAndCount;


public class EventOccurrenceStore implements EventOccurrenceEventStore<Bytes,byte[]> {


    private static final int SEQNUM_SIZE = 4;

    private final String name;

    private long streamTime = 0L;


    private final String metricScope;
    private InternalProcessorContext context;
    private Sensor expiredRecordSensor;

    private long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;

    private final long retentionPeriod;





    // New Structure
    private final ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<IInterval<Long>, long[]>> compositeEvents = new ConcurrentSkipListMap<>();
    private final ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, Set<byte[]>>> singleEvents = new ConcurrentSkipListMap<>();
    private final ConcurrentNavigableMap<Bytes, IntervalTree> comparationBase = new ConcurrentSkipListMap<>();

    private final Set<EventOccurrenceIteratorWrapper> openIterators = ConcurrentHashMap.newKeySet();
    private final Long withinMs;

    public EventOccurrenceStore(String name, String metricScope, long retentionPeriod, long windowSize, boolean retainDuplicates, long numberPreds, long numberSucc, Long withinMs) {
        this.name = name;
        this.metricScope = metricScope;
        this.retentionPeriod = retentionPeriod;

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
        put(key, value, counter);
        counter++;
    }



    private static org.apache.kafka.common.utils.Bytes wrapForDups(final org.apache.kafka.common.utils.Bytes key, final int seqnum) {
        final ByteBuffer buf = ByteBuffer.allocate(key.get().length + SEQNUM_SIZE);
        buf.put(key.get());
        buf.putInt(seqnum);

        return org.apache.kafka.common.utils.Bytes.wrap(buf.array());
    }

    private void removeExpiredSegments() {
        long minLiveTime = Math.max(0L, observedStreamTime - retentionPeriod + 1);
//        for (final FollowedByWindowStore.FollowedByWindowStoreIteratorWrapper it : openIterators) {
//            minLiveTime = Math.min(minLiveTime, context.timestamp());
//        }
       // WindowValueMap.headMap(minLiveTime, false).clear();
    }


    @Override
    public byte[] fetch(Bytes key, long windowStartTimestamp) {

        /*
        Objects.requireNonNull(key, "key cannot be null");

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

        }

         */

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

    @Override
    public void putIntervalEvent(Bytes key, byte[] value, long start, long end, boolean allowOverlaps) {

        garbageCollection();

        IInterval<Long> interval = new LongInterval(start,end);
        comparationBase.computeIfAbsent(key, bytes -> IntervalTreeBuilder.newBuilder()
                .usePredefinedType(IntervalTreeBuilder.IntervalType.LONG)
                .collectIntervals(interval1 -> new ListIntervalCollection())
                .build());
        comparationBase.get(key).add(new LongInterval(start, end));

        singleEvents.computeIfAbsent(key, bytes -> new ConcurrentSkipListMap<>());
        singleEvents.get(key).computeIfAbsent(end, new Function<Long, HashSet<byte[]>>() {
            @Override
            public HashSet<byte[]> apply(Long aLong) {
                return new HashSet<byte[]>();
            }
        });

        singleEvents.get(key).get(end).add(value);
        //System.out.println(key.toString().concat("_") + i++);

    }

    int i=0;

    private void garbageCollection(){
        if(streamTime<context.timestamp())
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
        });
    }

    @Override
    public void putCompositeEvent(Bytes key, long[] timestamps) {

        garbageCollection();

        IInterval<Long> interval = new LongInterval(timestamps[0], timestamps[timestamps.length - 1]);
        comparationBase.computeIfAbsent(key, bytes -> IntervalTreeBuilder.newBuilder()
                .usePredefinedType(IntervalTreeBuilder.IntervalType.LONG)
                .collectIntervals(interval1 -> new ListIntervalCollection())
                .build());
        comparationBase.get(key).add(new LongInterval(timestamps[0], timestamps[timestamps.length-1]));

        compositeEvents.computeIfAbsent(key, bytes -> new ConcurrentSkipListMap<>(new Comparator<IInterval<Long>>() {
            @Override
            public int compare(IInterval<Long> o1, IInterval<Long> o2) {
                return o1.getNormEnd().compareTo(o2.getNormEnd());
            }
        }));
        compositeEvents.get(key).put(interval, timestamps);

    }



    @Override
    public void put(Bytes key, byte[] value, long timestamp) {
        putIntervalEvent(key, value, timestamp,timestamp, false);
    }



    long lastCompositeGarbaging=0;

    @Override
    public KeyValueIterator<Bytes, long[]> fetchEvents(Bytes key, long timestamp) {

        garbageCollection();

        return new EventOccurrenceIteratorWrapper<long[]>(comparationBase.get(key).overlapStream(new LongInterval(timestamp,timestamp))
                .map(new Function<IInterval, Map.Entry<Bytes,long[]>>() {
                    @Override
                    public Map.Entry<Bytes, long[]> apply(IInterval interval) {
                        return new HashMap.SimpleEntry<>(key, compositeEvents.get(key).get(interval));
                    }
                }).iterator(),openIterators::remove, false);



    }

    @Override
    public KeyValueIterator<Bytes, byte[]> retrieveEvents(Bytes key, long[] timestamps) {

        garbageCollection();

        Iterator<Map.Entry<Bytes, byte[]>> it = Arrays.stream(timestamps).boxed()
                .map((Function<Long, Map.Entry<Bytes, byte[]>>) aLong ->
                            new HashMap.SimpleEntry<Bytes, byte[]>(key,singleEvents.get(key).get(aLong)
                        .iterator().next())).iterator();

        return new EventOccurrenceIteratorWrapper<byte[]>(it,openIterators::remove, false);
    }





    interface ClosingCallback {
        void deregisterIterator(final EventOccurrenceStore.EventOccurrenceIteratorWrapper iterator);
    }
    private static org.apache.kafka.common.utils.Bytes getKey(final org.apache.kafka.common.utils.Bytes keyBytes) {
        final byte[] bytes = new byte[keyBytes.get().length  - SEQNUM_SIZE];
        System.arraycopy(keyBytes.get(), 0, bytes, 0, bytes.length);
        return org.apache.kafka.common.utils.Bytes.wrap(bytes);

    }

    private static class EventOccurrenceIteratorWrapper<V>  implements KeyValueIterator<Bytes,V>{

        private Iterator<? extends Map.Entry<Bytes, V>> recordIterator;

        private Map.Entry<Bytes, V> next;
        private long currentTime;

        private final boolean retainDuplicates;
        private final EventOccurrenceStore.ClosingCallback callback;

        EventOccurrenceIteratorWrapper(final Iterator<Map.Entry<Bytes, V>> recordIterator,
                                       final EventOccurrenceStore.ClosingCallback callback,
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

