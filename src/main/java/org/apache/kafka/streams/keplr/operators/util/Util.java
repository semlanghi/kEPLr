package org.apache.kafka.streams.keplr.operators.util;

import com.brein.time.timeintervals.intervals.IInterval;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.zookeeper.server.SessionTracker;

public class Util {

    public static SessionWindow sessionFromInterval(IInterval<Long> interval){
        return new SessionWindow(interval.getNormStart(), interval.getNormEnd());
    }
}
