package Clock;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Physical Clock abstraction providing process system time (simulated via libfaketime in Docker)
 * and application-level synchronized time via Cristian's offset calculation.
 */
public class PhysicalClock {

    private static volatile long clockOffsetMs = 0;

    /**
     * Set the physical clock offset computed by Cristian's algorithm.
     */
    public static void setClockOffsetMs(long offset) {
        clockOffsetMs = offset;
    }

    /**
     * Get the current physical clock offset in milliseconds.
     */
    public static long getClockOffsetMs() {
        return clockOffsetMs;
    }

    /**
     * Get raw local process physical time in milliseconds.
     */
    public static long getLocalPhysicalTimeMillis() {
        return System.currentTimeMillis();
    }

    /**
     * Get estimated/synchronized reference physical time in milliseconds.
     */
    public static long getSynchronizedPhysicalTimeMillis() {
        return System.currentTimeMillis() + clockOffsetMs;
    }

    /**
     * Format a timestamp in milliseconds to yyyy-MM-dd HH:mm:ss.SSS.
     */
    public static String formatTime(long timestampMs) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return sdf.format(new Date(timestampMs));
    }

    /**
     * Returns local process physical time as formatted string.
     */
    public static String getLocalPhysicalTimeFormatted() {
        return formatTime(getLocalPhysicalTimeMillis());
    }

    /**
     * Returns application-level synchronized physical time as formatted string.
     */
    public static String getSynchronizedPhysicalTimeFormatted() {
        return formatTime(getSynchronizedPhysicalTimeMillis());
    }
}
