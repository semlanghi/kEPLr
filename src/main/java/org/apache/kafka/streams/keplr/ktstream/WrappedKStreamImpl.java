package org.apache.kafka.streams.keplr.ktstream;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.keplr.etype.EType;
import org.apache.kafka.streams.keplr.etype.TypedKey;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.internals.*;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorGraphNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.To;
import utils.OldTypedKeySerde;

import java.util.*;

public class WrappedKStreamImpl<K,V> extends AbstractStream<K,V> {

    private static final String MATCH_NAME = "KSTREAM-MATCH-";
    private static final String TYPEDKEY_MAP_NAME = "KSTREAM-TYPEDKEYMAP-";

    public WrappedKStreamImpl(KStreamImpl<K,V> stream) {
        super(stream);
    }

    public KTStream<K, V>[] match(final EType<K,V>... eTypes) {
        if (eTypes.length == 0) {
            throw new IllegalArgumentException("you must provide at least one type");
        }
        for (final Predicate<? super K, ? super V> predicate : eTypes) {
            Objects.requireNonNull(predicate, "types can't have null values");
        }

        final String branchName = builder.newProcessorName(MATCH_NAME);

        final String[] childNames = new String[eTypes.length];
        for (int i = 0; i < eTypes.length; i++) {
            childNames[i] = builder.newProcessorName(TYPEDKEY_MAP_NAME+eTypes[i].getDescription()+"-");
        }

        final ProcessorParameters<K,V> processorParameters = new ProcessorParameters<>(new KStreamMatch<>(eTypes.clone(), childNames), branchName);
        final ProcessorGraphNode<K, V> branchNode = new ProcessorGraphNode<>(branchName, processorParameters);
        builder.addGraphNode(this.streamsGraphNode, branchNode);

        final KTStream<K, V>[] branchChildren = new KTStreamImpl2[eTypes.length];


        for (int i = 0; i < eTypes.length; i++) {
            try {
                final EType<K,V> mappableType = eTypes[i].clone();
                final KStreamMap<K, V, TypedKey<K>, V> kStreamMap = new KStreamMap<>((key, value) -> new KeyValue<>(mappableType.typed(key), value));
                final ProcessorParameters<K, V> innerProcessorParameters = new ProcessorParameters<>(kStreamMap, childNames[i]);

                final ProcessorGraphNode<K, V> typizationNode = new ProcessorGraphNode<>(childNames[i], innerProcessorParameters);

                builder.addGraphNode(branchNode, typizationNode);
                branchChildren[i] = new KTStreamImpl2<K,V>(childNames[i], new OldTypedKeySerde<K>(keySerde), valSerde, sourceNodes, false, typizationNode, builder, eTypes[i]);

            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            }
        }

        return branchChildren;
    }

    private static class KStreamMatch<K, V> implements ProcessorSupplier<K, V> {

        private final EType<K, V>[] types;
        private final String[] childNodes;

        KStreamMatch(final EType<K, V>[] types,
                     final String[] childNodes) {
            this.types = types;
            this.childNodes = childNodes;
        }

        @Override
        public Processor<K, V> get() {
            return new KStreamMatchProcessor();
        }

        private class KStreamMatchProcessor extends AbstractProcessor<K, V> {
            @Override
            public void process(final K key, final V value) {
                for (int i = 0; i < types.length; i++) {
                    if (types[i].test(key, value)) {
                        context().forward(key, value, To.child(childNodes[i]));
                    }
                }
            }
        }
    }
}
