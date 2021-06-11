package org.apache.kafka.streams.keplr.etype;

import org.apache.kafka.streams.keplr.operators.IntervalEventOccurrenceSupplier;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.BinaryOperator;

/**
 * Class to abstract the concept of event type, the creation of the typed key depends
 * also on the type and is overridden according to the actual {@link EType}.
 * It is seen as a predicate, presenting the {@link Predicate#test(Object, Object)}
 * method, which take the events and returns whether a specific event present this type or not.
 * The {@link EType#description} attribute is the so called "Type Identifier", which is unique
 * for each type.
 * The {@link EType#onEvery} depicts whether the underlining stream is under the effect of an every.
 *
 *
 * @see Predicate
 * @param <K> The key class
 * @param <V> The value class
 */
public abstract class EType<K, V> implements Predicate<K, V> {

    protected String description;
    protected boolean onEvery;
    protected boolean chunkLeft = true;
    protected boolean chunkRight = true;

    public EType() {
    }

    protected EType(String description, boolean onEvery, boolean chunkLeft, boolean chunkRight) {
        this.description = description;
        this.onEvery = onEvery;
        this.chunkLeft = chunkLeft;
        this.chunkRight = chunkRight;
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

    /**
     * Factory method to create an every-enabled version of the current type.
     */
    public abstract EType<K, V> everyVersion();

    /**
     * Method to set up the two parameters, to set up the chunking
     * policy, and whether the overlapping should be checked either
     * on the right or the left.
     *
     * @param chunkLeft Disable overlapping on the left
     * @param chunkRight Disable overlapping on the right
     */
    public void chunk(boolean chunkLeft, boolean chunkRight) {
        this.chunkLeft = chunkLeft;
        this.chunkRight = chunkRight;
    }

    /**
     * Method to check if the current event is marked with the end condition.
     *
     * @param value The current event
     * @return Whether this event is the final one
     */
    public abstract boolean isThisTheEnd(V value);

    public EType(String description) {
        this.description = description;
    }

    public EType(EType<?, ?>[] types) {
        this.description = Arrays.stream(types)
                .map(eType -> eType.description)
                .reduce(new BinaryOperator<String>() {
                    @Override
                    public String apply(String s, String s2) {
                        return s + "_X_" + s2;
                    }
                }).get();
        this.onEvery = false;
    }

    /**
     * Factory method to create the typed key related to the current type.
     * @param key The key to be wrapped.
     * @return The typed
     */
    public abstract TypedKey<K> typed(K key);

    public abstract K untyped(TypedKey<K> typedKey);

    /**
     * Method to extract the start timestamp from the current event.
     * @param value The current event.
     * @return The start timestamp
     */
    public abstract long start(V value);

    /**
     * Method to extract the end timestamp from the current event.
     * @param value The current event.
     * @return The end timestamp
     */
    public abstract long end(V value);

    public long duration(V value) {
        return end(value) - start(value);
    }

    /**
     * A factory method to create the type product.
     * @param otherType The other type from which the type product is formed.
     * @param array
     * @return The type product
     */
    public abstract EType<K, V> product(EType<K, V> otherType, boolean array);

    public abstract EType<K, V> union(EType<K, V> otherType);

    /**
     * Method to create the current type-related joiner. This method is valid only if
     * current type is a composite type, i.e., a type product.
     * @return The {@link ValueJoiner} related to this type, if it is composite.
     */
    public abstract ValueJoiner<V, V, V> joiner();

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

    /**
     * A method to extract events from a composite accumiulated events. Used in the
     * event accumulation.
     *
     * @see IntervalEventOccurrenceSupplier
     * @param value The composite events, formed by an array of events of the same type.
     * @return An {@link ArrayList} of events.
     */
    public abstract ArrayList<V> extract(V value);

    /**
     * A method to wrap multiple events of the same type inside a composite event.
     * It can be considered as the {@link ValueJoiner} for the event accumulation.
     *
     * @see IntervalEventOccurrenceSupplier
     * @param value
     * @return
     */
    public abstract V wrap(ArrayList<V> value);

    @Override
    public abstract EType<K,V> clone() throws CloneNotSupportedException;
}
