package keplr.etype;

import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BinaryOperator;

public class ETypeString extends EType<String,String>{

    public ETypeString() {
    }

    private ETypeString(String description, boolean onEvery, boolean chunkLeft, boolean chunkRight) {
        super(description, onEvery, chunkLeft, chunkRight);
    }

    @Override
    public EType<String, String> everyVersion() {
        EType<String,String> type =  new ETypeString(this.description);
        type.setOnEvery(true);
        type.setChunkLeft(this.isChunkLeft());
        type.setChunkRight(this.isChunkRight());
        return type;
    }

    @Override
    public boolean isThisTheEnd(String value) {
        return false;
    }

    public ETypeString(EType[] types) {
        super(types);
    }

    public ETypeString(String name) {
        super(name);
    }

    @Override
    public ArrayList<String> extract(String value) {
        return (ArrayList<String>) Arrays.asList(value.split("\\.")[1].split("/"));
    }

    @Override
    public String wrap(ArrayList<String> value) {
        return value.stream().reduce(new BinaryOperator<String>() {
            @Override
            public String apply(String s, String s2) {
                return s+"/"+s2;
            }
        }).get();
    }

    @Override
    public EType<String, String> clone() throws CloneNotSupportedException {
        return new ETypeString(description, onEvery, chunkLeft, chunkRight);
    }


    @Override
    public TypedKey<String> typed(String key) {
        return new TypedKey<>(key,this.description);
    }



    @Override
    public String untyped(TypedKey<String> typedKey) {
        return typedKey.getKey();
    }

    @Override
    public long start(String value) {
        return  Long.parseLong(value.split("\\.")[0].substring(1));
    }

    @Override
    public long end(String value) {
        String[] chunked = value.split("\\.");
        return Long.parseLong(chunked[chunked.length-1]);
    }


    @Override
    public EType<String, String> product(EType<String, String> otherType, boolean array) {
        EType[] array1 = {this,otherType};
        return new ETypeString(array1);
    }

    @Override
    public EType<String, String> union(EType<String, String> otherType) {
        Set<EType<String,String>> temp = new HashSet<>();
        temp.add(this);
        temp.add(otherType);
        return new UnionEType<>(temp, this.onEvery || otherType.isOnEvery());
    }

    @Override
    public ValueJoiner<String, String, String> joiner() {
        return (value1, value2) -> value1 + "_followedBy_" + value2;
    }

    @Override
    public Class<String> kClass() {
        return String.class;
    }

    @Override
    public boolean test(String key, String value) {
        return value.substring(0,1).equals(this.description);
    }




}
