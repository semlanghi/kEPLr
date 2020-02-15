package org.apache.kafka.streams;

import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

public class PatternWindow extends Window {


    /**
     * Create a new window for the given start time (inclusive) and end time (exclusive).
     *
     * @param startMs the start timestamp of the window (inclusive)
     * @param endMs   the end timestamp of the window (exclusive)
     * @throws IllegalArgumentException if {@code startMs} is negative or if {@code endMs} is smaller than or equal to
     *                                  {@code startMs}
     */
    public PatternWindow(long startMs, long endMs) throws IllegalArgumentException {
        super(startMs, endMs);
    }

    @Override
    public boolean overlap(Window other) {
        if (getClass() != other.getClass()) {
            throw new IllegalArgumentException("Cannot compare windows of different type. Other window has type "
                    + other.getClass() + ".");
        }
        final TimeWindow otherWindow = (TimeWindow) other;
        return startMs < other.end() && other.start() < endMs;
    }


}
