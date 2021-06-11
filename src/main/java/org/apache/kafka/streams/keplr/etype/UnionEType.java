package org.apache.kafka.streams.keplr.etype;

import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;

public class UnionEType<K,V> extends EType<K,V>{

    private Set<EType<K,V>> types = new HashSet<>();

    public UnionEType(Set<EType<K, V>> types, boolean onEvery) {
        this.types = types;
        this.description = types.stream().reduce("", (s, kveType) -> kveType.description, (s, s2) -> s+"_U_"+s2);
        this.onEvery = onEvery;
    }

    @Override
    public EType<K, V> everyVersion() {
        return new UnionEType<>(types,true);
    }

    @Override
    public boolean isThisTheEnd(V value) {
        return types.stream().anyMatch(kveType -> kveType.isThisTheEnd(value));
    }

    @Override
    public TypedKey<K> typed(K key) {
        return null;
    }

    @Override
    public K untyped(TypedKey<K> typedKey) {
        Optional<EType<K,V>> compatibleType = types.stream().findFirst();
        return compatibleType.map(kveType -> kveType.untyped(typedKey)).orElse(null);
    }

    @Override
    public long start(V value) {
        return 0;
    }

    @Override
    public long end(V value) {
        return 0;
    }

    @Override
    public EType<K, V> product(EType<K, V> otherType, boolean array) {
        return null;
    }

    @Override
    public EType<K, V> union(EType<K, V> otherType) {
        return null;
    }

    @Override
    public ValueJoiner<V, V, V> joiner() {
        return null;
    }

    @Override
    public Class<K> kClass() {
        return null;
    }

    @Override
    public ArrayList<V> extract(V value) {
        return null;
    }

    @Override
    public V wrap(ArrayList<V> value) {
        return null;
    }

    @Override
    public EType<K, V> clone() throws CloneNotSupportedException {
        return null;
    }

    @Override
    public boolean test(K key, V value) {
        return false;
    }
}
