package org.apache.kafka.streams.keplr.operators.util;

import org.apache.kafka.streams.keplr.etype.EType;
import org.apache.kafka.streams.keplr.etype.TypedKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class Automaton<K> {

    private Set<Integer> finalStates;
    private Set<EType<K,?>> retrievalSet;
    private NavigableMap<Integer, State<K>> states;

    public Automaton() {
        this.finalStates = new HashSet<>();
        this.states = new TreeMap<>();
    }

    public Automaton(Set<Integer> finalStates, NavigableMap<Integer, State<K>> states) {
        this.finalStates = finalStates;
        this.states = states;
    }

    public void addState(State<K> state){
        if(state.isTerminal())
            finalStates.add(state.getId());
        else retrievalSet.add(state.getSearched());
        if(!states.isEmpty())
            state.setSucc(states.lastEntry().getValue());
        states.put(state.getId(), state);
    }

    public void addState(State<K> state, State<K> successor){
        if(state.isTerminal())
            finalStates.add(state.getId());
        else
        state.setSucc(successor);
        states.put(state.getId(), state);
    }

    public State<K> getStateFromId(int id){
        return states.get(id);
    }

    public State<K> startingState(){
        return states.firstEntry().getValue();
    }
}
