package com.ververica.field.windowing.assigners;

import com.ververica.field.dynamicrules.Rule;
import com.ververica.field.windowing.windows.TimeWindow;
import com.ververica.field.windowing.windows.Window;
import java.io.Serializable;

public abstract class WindowAssigner<W extends Window> implements Serializable {
    public static TimeWindow assignWindow(long currentTime, Rule rule) {
        switch (rule.getWindowType()) {
            case GROWTH_WINDOW:
                return assignFixedStartingWindow(currentTime, rule.getWindowMillis());
            default:
                return assignNonFixedStartingWindow(currentTime, rule.getWindowMillis());
        }
    }

    public static TimeWindow assignNonFixedStartingWindow(long currentTime, long windowSize) {
        long start = currentTime -  windowSize;
        return new TimeWindow(start, currentTime);
    }

    //TODO: 需要考虑时区
    public static TimeWindow assignFixedStartingWindow(long currentTime, long windowSize) {
        long start = TimeWindow.getWindowStartWithOffset(currentTime, 0, windowSize);
        long end = start + windowSize;
        return new TimeWindow(start, end);
    }
}