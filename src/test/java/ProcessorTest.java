import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.keplr.etype.TypedKey;
import org.apache.kafka.streams.keplr.operators.GatewayProcessorSupplier;
import org.apache.kafka.streams.keplr.operators.KEPLrProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.ChangeLoggingKeyValueBytesStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import utils.TypedKeySerde2;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

public class ProcessorTest {

    @Mock
    private ProcessorContext context;

    @BeforeEach
    public void setUp(){
        this.context = Mockito.mock(ProcessorContextImpl.class);
    }

    @Test
    public void gatewayTest(){
        KeyValueStore<String, Integer> store =
                Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("test-Store"), Serdes.String(), Serdes.Integer()).withLoggingDisabled().build();
        when(this.context.getStateStore(Mockito.anyString())).thenReturn(store);
        when(this.context.applicationId()).thenReturn("test-app");
        when(this.context.metrics()).thenReturn(new StreamsMetricsImpl(new Metrics(),Thread.currentThread().getName()));
        when(this.context.taskId()).thenReturn(new TaskId(0,0));
        ArgumentCaptor<String> valueCapture = ArgumentCaptor.forClass(String.class);
        doNothing().when(this.context).forward(Mockito.any(), valueCapture.capture());

        GatewayProcessorSupplier<String,String> gatewayProcessorSupplier =
                new GatewayProcessorSupplier<>(null, "test-Store");

        KEPLrProcessor<String,String> processor = gatewayProcessorSupplier.get();
        store.init(context, null);
        processor.init(context);

        processor.process(new TypedKey<>("key-1", null),"value-1");
        assertEquals(valueCapture.getValue(),"value-1");

        store.put("key-1", 0);
        processor.process(new TypedKey<>("key-1", null),"value-2");
        assertEquals(valueCapture.getValue(),"value-1");

        store.put("key-1", 1);
        processor.process(new TypedKey<>("key-1", null),"value-2");
        assertEquals(valueCapture.getValue(),"value-2");
    }

    @Test
    public void followedByTest(){
        KeyValueStore<String, Integer> gateStore =
                Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("test-Store"), Serdes.String(), Serdes.Integer()).withLoggingDisabled().build();
        KeyValueStore<String, Integer> gateStore1 =
                Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("test-Store1"), Serdes.String(), Serdes.Integer()).withLoggingDisabled().build();
        KeyValueStore<String, Integer> windowStore =
                Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("test-winStore"), Serdes.String(), Serdes.Integer()).withLoggingDisabled().build();
        when(this.context.getStateStore("test-Store")).thenReturn(gateStore);
        when(this.context.getStateStore("test-Store1")).thenReturn(windowStore);
        when(this.context.applicationId()).thenReturn("test-app");
        when(this.context.metrics()).thenReturn(new StreamsMetricsImpl(new Metrics(),Thread.currentThread().getName()));
        when(this.context.taskId()).thenReturn(new TaskId(0,0));
        ArgumentCaptor<String> valueCapture = ArgumentCaptor.forClass(String.class);
        doNothing().when(this.context).forward(Mockito.any(), valueCapture.capture());

        GatewayProcessorSupplier<String,String> gatewayProcessorSupplier =
                new GatewayProcessorSupplier<>(null, "test-Store");
        GatewayProcessorSupplier<String,String> gatewayProcessorSupplier1 =
                new GatewayProcessorSupplier<>(null, "test-Store1");

        KEPLrProcessor<String,String> processor = gatewayProcessorSupplier.get();
        KEPLrProcessor<String,String> processor1 = gatewayProcessorSupplier.get();

        gateStore.init(context, null);
        processor.init(context);
        processor1.init(context);

        processor.process(new TypedKey<>("key-1", null),"value-1");
        assertEquals(valueCapture.getValue(),"value-1");

        gateStore.put("key-1", 0);
        processor.process(new TypedKey<>("key-1", null),"value-2");
        assertEquals(valueCapture.getValue(),"value-1");

        gateStore.put("key-1", 1);
        processor.process(new TypedKey<>("key-1", null),"value-2");
        assertEquals(valueCapture.getValue(),"value-2");
    }
}
