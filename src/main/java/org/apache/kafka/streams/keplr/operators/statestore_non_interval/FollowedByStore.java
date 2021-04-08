package org.apache.kafka.streams.keplr.operators.statestore_non_interval;

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

/**
 * Byte-based implementation of the {@link FollowedByEventStore}. It uses three different structures to store the events.
 * Events are stored as "interval events", i.e. events with a duration. Each event is defined by their {@link IInterval},
 * and their (Typed)key, here represented in {@link Bytes}.
 * The {@link IntervalTree} structure is used to keep all intervals for a certain key, and for retrieving the
 * overlapping intervals with a given one.
 * We also keep a structure to maintain the last iteration of for a specific event. //TODO what is that??? What for???
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

    //This one contains eventTrees for same (Typed)key
    private final ConcurrentNavigableMap<Bytes, IntervalTree> comparationBase = new ConcurrentSkipListMap<>();

    //This one contains events themselves (values) for the current (Typed)key and Interval
    private final ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>> immutableEvents = new ConcurrentSkipListMap<>();
    //what is this??? contains end of all current intervals??? for what purpose???

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

//        final StreamsMetricsImpl metrics = this.context.metrics();
//        final String taskName = context.taskId().toString();
//        expiredRecordSensor = metrics.storeLevelSensor(
//                taskName,
//                name(),
//                EXPIRED_WINDOW_RECORD_DROP,
//                Sensor.RecordingLevel.INFO
//        );
//        addInvocationRateAndCount(
//                expiredRecordSensor,
//                "stream-" + metricScope + "-metrics",
//                metrics.tagMap("task-id", taskName, metricScope + "-id", name()),
//                EXPIRED_WINDOW_RECORD_DROP
//        );

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

        putIntervalEvent(key, value, timestamp, true);
    }

    private static Bytes wrapForDups(final Bytes key, final int seqnum) {
        final ByteBuffer buf = ByteBuffer.allocate(key.get().length + SEQNUM_SIZE);
        buf.put(key.get());
        buf.putInt(seqnum);

        return Bytes.wrap(buf.array());
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
    public void putIntervalEvent(Bytes key, byte[] value, long timestamp, boolean allowOverlaps) {
        //TODO why compute only if absent - this need some explanation??? compute if key is missing? seems fishy
        //define how comparison is done for immutable events
        immutableEvents.computeIfAbsent(key, bytes -> new ConcurrentSkipListMap<>());
        immutableEvents.get(key).put(timestamp,value);
    }

    long lastCompositeGarbaging=0;

    public void garbageCollectorComposite(Bytes key, long timestamp){

        if(immutableEvents.get(key)!=null) {
            immutableEvents.get(key).headMap(timestamp).clear();

        }
    }



    private ConcurrentSkipListSet<Long> toDelete= new ConcurrentSkipListSet();
    @Override
    public KeyValueIterator<Bytes, byte[]> fetchEventsInLeft(Bytes key, long timestamp, boolean delete) {
        //TODO what is the usecase? should we still have start and end times for fetch???
        //TODO timestamp included or excluded???


        IInterval<Long> searchInterval = new LongInterval(timestamp-withinMs, timestamp);

        //TODO ASK samuele-what is lastCompositeGarbaging, for now end->timestamp
        if(lastCompositeGarbaging+withinMs*10<timestamp){
            garbageCollectorComposite(key,timestamp);
            lastCompositeGarbaging = timestamp;
        }

        immutableEvents.computeIfAbsent(key, bytes -> new ConcurrentSkipListMap<>());
        ConcurrentNavigableMap<Long, byte[]> map = immutableEvents.get(key).headMap(timestamp);

        //get intervals that fell inside searchinterval then filter further based on ...
        Iterator<Map.Entry<Bytes, byte[]>> it = immutableEvents.get(key)
            .tailMap(timestamp-withinMs)
            .keySet()
            .stream()
            .filter(timestamp2 -> {
                if (delete) {toDelete.add(timestamp2);}
                return true;
            })
            .map((Function<Long, Map.Entry<Bytes, byte[]>>) timestamp2 -> new HashMap.SimpleEntry<>(key,immutableEvents.get(key).get(timestamp2))).iterator();

        //TODO ASK SAMUELE, so we delete all events with the current type??? not just one specific event???
        //TODO WHY WAS THERE SORTED IF THIS IS concurrentnavigatablemap

        toDelete.forEach(t -> {
            immutableEvents.get(key).remove(t);
        });

        toDelete.clear();

        return new FollowedByWindowStoreIteratorWrapper<>(it,openIterators::remove, false);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> fetchEventsInRight(Bytes key, long timestamp) {
        //TODO: IS PLACEHOLDER
        return fetchEventsInLeft(key,timestamp,false);
    }


    interface ClosingCallback {
        void deregisterIterator(final FollowedByStore.FollowedByWindowStoreIteratorWrapper iterator);
    }
    private static Bytes getKey(final Bytes keyBytes) {
        final byte[] bytes = new byte[keyBytes.get().length  - SEQNUM_SIZE];
        System.arraycopy(keyBytes.get(), 0, bytes, 0, bytes.length);
        return Bytes.wrap(bytes);

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
