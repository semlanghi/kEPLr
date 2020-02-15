package org.apache.kafka.streams.keplr.etype;

import java.util.Objects;

public class TypedKey<K> {


    protected K key;
    //protected EType<K,?> type;
    protected String type;

    public TypedKey() {
    }

    public TypedKey(K key, String type) {
        this.key = key;
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public K getKey(){
        return key;
    };

    public void setKey(K key) {
        this.key = key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TypedKey)) return false;
        TypedKey<?> typedKey = (TypedKey<?>) o;
        return key.equals(typedKey.key) &&
                type.equals(typedKey.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, type);
    }



}
