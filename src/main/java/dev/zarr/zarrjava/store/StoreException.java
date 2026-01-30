package dev.zarr.zarrjava.store;

/**
 * Exception thrown when store operations fail.
 * Provides context about which store and operation failed.
 */
public class StoreException extends RuntimeException {

    public StoreException(String message) {
        super(message);
    }

    public StoreException(String message, Throwable cause) {
        super(message, cause);
    }

    public static StoreException readFailed(String storePath, String[] keys, Throwable cause) {
        return new StoreException(
                String.format("Failed to read from store '%s' at key '%s': %s",
                        storePath, String.join("/", keys), cause.getMessage()),
                cause);
    }

    public static StoreException writeFailed(String storePath, String[] keys, Throwable cause) {
        return new StoreException(
                String.format("Failed to write to store '%s' at key '%s': %s",
                        storePath, String.join("/", keys), cause.getMessage()),
                cause);
    }

    public static StoreException deleteFailed(String storePath, String[] keys, Throwable cause) {
        return new StoreException(
                String.format("Failed to delete from store '%s' at key '%s': %s",
                        storePath, String.join("/", keys), cause.getMessage()),
                cause);
    }

    public static StoreException listFailed(String storePath, String[] keys, Throwable cause) {
        return new StoreException(
                String.format("Failed to list store contents at '%s' under key '%s': %s",
                        storePath, String.join("/", keys), cause.getMessage()),
                cause);
    }
}
