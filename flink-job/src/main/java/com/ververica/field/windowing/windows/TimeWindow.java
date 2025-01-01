package com.ververica.field.windowing.windows;

import java.text.SimpleDateFormat;

public class TimeWindow extends Window {
    private static final ThreadLocal<SimpleDateFormat> timeFormat= ThreadLocal.withInitial( ()->new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));
    private final long start;
    private final long end;

    public TimeWindow(long start, long end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public long maxTimestamp() {
        return 0;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    /**
     * 获取窗口起始时间戳的方法
     *
     * @param timestamp 用于计算窗口起始时间的时间戳
     * @param offset 计算窗口起始时间的偏移量
     * @param windowSize 生成的窗口的大小。
     * @return 窗口起始时间戳
     */
    public static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
        final long remainder = (timestamp - offset) % windowSize;
        // handle both positive and negative cases
        if (remainder < 0) {
            return timestamp - (remainder + windowSize);
        } else {
            return timestamp - remainder;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TimeWindow that = (TimeWindow) o;

        if (start != that.start) return false;
        return end == that.end;
    }

    @Override
    public int hashCode() {
        int result = (int) (start ^ (start >>> 32));
        result = 31 * result + (int) (end ^ (end >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "TimeWindow{" +
                "start=" + timeFormat.get().format(start) +
                ", end=" + timeFormat.get().format(end) +
                '}';
    }
}
