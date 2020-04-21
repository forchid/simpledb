package simpledb;

/** Represents fatal runtime exceptions in simple db intern such as IO exception.
 *
 * @author little-pan
 */
public class DbException extends RuntimeException {

    public DbException(String message) {
        super(message);
    }

    public DbException(String message, Throwable cause) {
        super(message, cause);
    }

}
