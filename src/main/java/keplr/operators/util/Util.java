package keplr.operators.util;

import com.brein.time.timeintervals.intervals.IInterval;
import org.apache.kafka.streams.kstream.internals.SessionWindow;

public class Util {

    public static SessionWindow sessionFromInterval(IInterval<Long> interval){
        return new SessionWindow(interval.getNormStart(), interval.getNormEnd());
    }
}
