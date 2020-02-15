package org.apache.kafka.streams;

import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.BinaryOperator;

/**
 * Class to abstract the concept of event type, the creation of the typed key depends also on the type
 * and is overridden according to the actual {@code EType}.
 * @param <K>
 * @param <V>
 */

public abstract class EType<K,V> implements Predicate<K,V>{
    
    protected String description;
    private boolean onEvery;
    private boolean chunkLeft=true;
    private boolean chunkRight=true;

    public EType() {
    }

    public boolean isChunkLeft() {
        return chunkLeft;
    }

    public void setChunkLeft(boolean chunkLeft) {
        this.chunkLeft = chunkLeft;
    }

    public boolean isChunkRight() {
        return chunkRight;
    }

    public void setChunkRight(boolean chunkRight) {
        this.chunkRight = chunkRight;
    }

    public boolean isOnEvery() {
        return onEvery;
    }

    public void setOnEvery(boolean onEvery) {
        this.onEvery = onEvery;
    }

    public abstract EType<K,V> everyVersion();

    public void chunk(boolean chunkLeft, boolean chunkRight){
        this.chunkLeft = chunkLeft;
        this.chunkRight = chunkRight;
    }

    public EType(String description) {
        this.description = description;
    }

    public EType(EType<?,?>[] types){

        this.description = Arrays.stream(types)
                .map(eType -> eType.description)
                .reduce(new BinaryOperator<String>() {
                    @Override
                    public String apply(String s, String s2) {
                        return s+"_X_"+s2;
                    }
                }).get();
        this.onEvery = false;
    }

    public abstract TypedKey<K> typed(K key);

    //TODO: Factory typed key, see Guava

    public abstract K untyped(TypedKey<K> typedKey);

    public abstract long start(V value);

    public abstract long end(V value);

    public long duration(V value){
        return end(value)-start(value);
    }

    public abstract EType<K,V> product(EType<K, V> otherType, boolean array);

    public abstract ValueJoiner<V,V,V> joiner();

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EType)) return false;
        EType<?, ?> eType = (EType<?, ?>) o;
        return onEvery == eType.onEvery &&
                Objects.equals(description, eType.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(description);
    }

    @Override
    public String toString() {
        return "EType{" +
                "description='" + description + '\'' +
                ", onEvery=" + onEvery +
                ", chunkLeft=" + chunkLeft +
                ", chunkRight=" + chunkRight +
                '}';
    }

    public abstract Class<K> kClass();

    public abstract ArrayList<V> extract(V value);
    public abstract V wrap(ArrayList<V> value);


}
