package com.ververica.field.windowing.assigners;

import com.ververica.field.windowing.windows.TimeWindow;
import org.junit.Test;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class WindowAssignerTest {

    @Test
    public void assignGrowthStartingWindow() {
        long currentTime = 1734996346000L; // 2024-12-24 09:25:46
        long windowSize = TimeUnit.HOURS.toMillis(24); // 24h
        long start = 1734969600000L; // 2024-12-24 00:00:00
        long end = 1735056000000L; // 2024-12-25 00:00:00
        TimeWindow expectedTimeWindow = new TimeWindow(start, end);
        TimeWindow growthStartingWindow = WindowAssigner.assignGrowthStartingWindow(currentTime, windowSize);
        assertEquals(expectedTimeWindow, growthStartingWindow);
    }
}