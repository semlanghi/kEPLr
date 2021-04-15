import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.keplr.etype.TypedKey;
import org.apache.kafka.streams.keplr.operators.statestore_non_interval.FollowedByBytesStoreSupplierNew;
import org.apache.kafka.streams.keplr.operators.statestore_non_interval.FollowedByEventStoreNew;
import org.apache.kafka.streams.keplr.operators.statestore_non_interval.FollowedByStoreBuilderNew;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StoreBuilder;
import org.junit.jupiter.api.Test;
import utils.TypedKeySerde;

class StateStoreNonIntervalTest {
    @Test
    void FollowedByStoreTest() {

        //retention period: how long an event is kept at most???
        //windowSize:???
        //numberPreds:???
        //withinMs: how far must last event be to be considered followed by
        FollowedByBytesStoreSupplierNew storeSupplier = new FollowedByBytesStoreSupplierNew("_Store_TEST", 100, 100L, false, 5L, 5);
        final StoreBuilder<FollowedByEventStoreNew<TypedKey<String>,String>> supportStore = new FollowedByStoreBuilderNew<>(storeSupplier, new TypedKeySerde<String>(String.class), Serdes.String(), Time.SYSTEM);
        
        //FollowedByStore store = new FollowedByStore("test", "xxx", 5,5,false,5,5,5L);
        FollowedByEventStoreNew<TypedKey<String>, String> store = supportStore.build();
        store.init(null,null);
        //store.put(new TypedKey<>("TEST1", "TEST_TYPE"),"TEST_VAL",6L);
        //store.put(new TypedKey<>("TEST1", "TEST_TYPE"),"TEST_VAL2",9L);
        store.putEvent(new TypedKey<>("TEST1", "TEST_TYPE"),"TEST_VAL2",9L,true);
        store.putEvent(new TypedKey<>("TEST1", "TEST_TYPE"),"TEST_VAL6",10L,true);
        KeyValueIterator<TypedKey<String>, String> returnVal = store.fetchEventsInLeft(new TypedKey<String>("TEST1", "TEST_TYPE"), 5L, 15L, false);
        System.out.println(returnVal.hasNext());
        System.out.println(returnVal.next());
        System.out.println(returnVal.next());
    }
}
//occurance-accumulates events of the same type, helps to makes 4a instead of a->a->a->a
//followedby