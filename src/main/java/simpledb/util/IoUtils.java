package simpledb.util;

import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Io utils.
 *
 * @author little-pan
 */
public final class IoUtils {

    static final int ERROR_LEVEL = 3, INFO_LEVEL = 2, DEBUG_LEVEL = 1;
    static final int LOG_LEVEL = Integer.getInteger("simpledb.error.logLevel", INFO_LEVEL);

    static final ThreadLocal<DateFormat> TIME_FORMAT = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        }
    };

    private IoUtils() {
        // NOOP
    }

    public static void close(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                // Ignore it
            }
        }
    }

    public static void debug(PrintStream out, String format, Object... args) {
        if (DEBUG_LEVEL >= LOG_LEVEL) {
            String tag = "DEBUG";
            log(out, tag, null, format, args);
        }
    }

    public static void info(PrintStream out, String format, Object... args) {
        if (INFO_LEVEL >= LOG_LEVEL) {
            String tag = "INFO ";
            log(out, tag, null, format, args);
        }
    }

    public static void error(PrintStream out, String format, Object... args) {
        if (ERROR_LEVEL >= LOG_LEVEL) {
            String tag = "ERROR";
            log(out, tag, null, format, args);
        }
    }

    public static void error(PrintStream out, Throwable cause, String format, Object... args) {
        if (ERROR_LEVEL >= LOG_LEVEL) {
            String tag = "ERROR";
            log(out, tag, cause, format, args);
        }
    }

    static void log(PrintStream out, String tag, Throwable cause, String format, Object... args) {
        String message = String.format(format, args);
        String thread = Thread.currentThread().getName();
        DateFormat df = TIME_FORMAT.get();
        String time = df.format(new Date());
        message = String.format("%s[%s][%s] %s", time, tag, thread, message);
        if (cause == null) {
            out.println(message);
        } else {
            synchronized (out) {
                out.println(message);
                cause.printStackTrace(out);
            }
        }
    }

}
