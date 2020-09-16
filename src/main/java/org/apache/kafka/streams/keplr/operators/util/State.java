package org.apache.kafka.streams.keplr.operators.util;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.keplr.etype.EType;
import org.apache.kafka.streams.keplr.etype.TypedKey;

public class State<K> {

    private final int id;
    //ASSUMPTION: every state has just one outgoing arc
    private State<K> succ;
    private EType<K,?> searched;
    private final boolean terminal;

    public State(int id, EType<K,?> searched) {
        this.id = id;
        this.searched = searched;
        this.terminal = false;
    }

    public int getId() {
        return id;
    }

    public State(int id, boolean terminal) {
        this.id = id;
        this.terminal = terminal;
    }

    public void setSucc(State<K> succ) {
        this.succ = succ;
    }

    public EType<K, ?> getSearched() {
        return searched;
    }

    public boolean isTerminal() {
        return terminal;
    }

    public State<K> advance(TypedKey<K> key){
        return key.getType().equals(searched.getDescription()) ? this : succ;
    }
}
