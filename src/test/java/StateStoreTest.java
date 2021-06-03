import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.keplr.etype.TypedKey;
import org.apache.kafka.streams.keplr.operators.statestore.FollowedByBytesStoreSupplier;
import org.apache.kafka.streams.keplr.operators.statestore.FollowedByEventStore;
import org.apache.kafka.streams.keplr.operators.statestore.FollowedByStoreBuilder;
import org.apache.kafka.streams.keplr.operators.statestore_non_interval.FollowedByBytesStoreSupplierNew;
import org.apache.kafka.streams.keplr.operators.statestore_non_interval.FollowedByEventStoreNew;
import org.apache.kafka.streams.keplr.operators.statestore_non_interval.FollowedByStoreBuilderNew;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StoreBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import utils.TypedKeySerde;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

class StateStoreTest {

    @Mock
    private ProcessorContext context;

    @BeforeEach
    public void setUp(){
        this.context = Mockito.mock(ProcessorContextImpl.class);
    }

    @Test
    public void FollowedByStoreTest() {
        when(this.context.applicationId()).thenReturn("test-app");
        when(this.context.metrics()).thenReturn(new StreamsMetricsImpl(new Metrics(),Thread.currentThread().getName()));
        when(this.context.taskId()).thenReturn(new TaskId(0,0));

        FollowedByBytesStoreSupplier storeSupplier = new FollowedByBytesStoreSupplier("_Store_TEST", 100, 100L, false,
                1000L, 5);
        final StoreBuilder<FollowedByEventStore<TypedKey<String>,String>> supportStore = new FollowedByStoreBuilder<>(storeSupplier, new TypedKeySerde<String>(String.class), Serdes.String(), Time.SYSTEM);
        FollowedByEventStore<TypedKey<String>, String> store = supportStore.build();

        store.init(this.context,null);
        store.putIntervalEvent(new TypedKey<String>("TEST1", "TEST_TYPE"),"TEST_VAL",5L, 5L, true);
        store.putIntervalEvent(new TypedKey<String>("TEST1", "TEST_TYPE"),"TEST_VAL2",9L, 9L, true);
        KeyValueIterator<TypedKey<String>, String> returnVal = store.fetchEventsInLeft(new TypedKey<String>("TEST1", "TEST_TYPE"), 10L, 10L, false);
    }

    @Test
    public void FollowedByNonIntervalStoreTest() {

        when(this.context.applicationId()).thenReturn("test-app");
        when(this.context.metrics()).thenReturn(new StreamsMetricsImpl(new Metrics(),Thread.currentThread().getName()));
        when(this.context.taskId()).thenReturn(new TaskId(0,0));

        FollowedByBytesStoreSupplierNew storeSupplier = new FollowedByBytesStoreSupplierNew("_Store_TEST", 100, 100L, false, 5L, 5);
        final StoreBuilder<FollowedByEventStoreNew<TypedKey<String>,String>> supportStore = new FollowedByStoreBuilderNew<>(storeSupplier, new TypedKeySerde<String>(String.class), Serdes.String(), Time.SYSTEM);
        FollowedByEventStoreNew<TypedKey<String>, String> store = supportStore.build();

        store.init(this.context,null);
        store.putEvent(new TypedKey<>("TEST1", "TEST_TYPE"),"TEST_VAL2",9L);
        store.putEvent(new TypedKey<>("TEST1", "TEST_TYPE"),"TEST_VAL6",10L);

        KeyValueIterator<TypedKey<String>, String> returnVal = store.fetchEventsInLeft(new TypedKey<String>("TEST1", "TEST_TYPE"), 5L, 15L, false);

        System.out.println(returnVal.hasNext());
        System.out.println(returnVal.next());
        System.out.println(returnVal.next());
    }
}
