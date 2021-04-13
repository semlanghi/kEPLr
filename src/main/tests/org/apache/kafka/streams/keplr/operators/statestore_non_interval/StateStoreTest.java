package org.apache.kafka.streams.keplr.operators.statestore_non_interval;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.keplr.etype.TypedKey;
import org.apache.kafka.streams.keplr.operators.statestore.FollowedByBytesStoreSupplier;
import org.apache.kafka.streams.keplr.operators.statestore.FollowedByEventStore;
import org.apache.kafka.streams.keplr.operators.statestore.FollowedByStoreBuilder;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StoreBuilder;
import org.junit.jupiter.api.Test;
import utils.TypedKeySerde;

import static org.junit.jupiter.api.Assertions.*;
class StateStoreTest {
    @Test
    void FollowedByStoreTest() {

        org.apache.kafka.streams.keplr.operators.statestore.FollowedByBytesStoreSupplier storeSupplier = new FollowedByBytesStoreSupplier("_Store_TEST", 100, 100L, false, 5L, 5);
        final StoreBuilder<FollowedByEventStore<TypedKey<String>,String>> supportStore = new FollowedByStoreBuilder<>(storeSupplier, new TypedKeySerde<String>(String.class), Serdes.String(), Time.SYSTEM);
        
        //FollowedByStore store = new FollowedByStore("test", "xxx", 5,5,false,5,5,5L);
        FollowedByEventStore<TypedKey<String>, String> store = supportStore.build();
        store.init(null,null);
        store.put(new TypedKey<>("TEST1", "TEST_TYPE"),"TEST_VAL",6L);
        store.put(new TypedKey<>("TEST1", "TEST_TYPE"),"TEST_VAL2",9L);
        store.putIntervalEvent(new TypedKey<>("TEST1", "TEST_TYPE"),"TEST_VAL2",9L,9L,true);
        KeyValueIterator<TypedKey<String>, String> returnVal = store.fetchEventsInLeft(new TypedKey<String>("TEST1", "TEST_TYPE"), 10L, 12L, false);
        System.out.println(returnVal.hasNext());
        System.out.println(returnVal.next());
    }
}
//occurance-accumulates events of the same type, helps to makes 4a instead of a->a->a->a
//followedby