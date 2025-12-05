package dev.zarr.zarrjava.store;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;


/** A Store implementation that buffers reads and writes and flushes them to an underlying Store as a zip file.
 */
public class BufferedZipStore implements Store, Store.ListableStore {

    private final StoreHandle underlyingStore;
    private final Store.ListableStore bufferStore;

    private void writeBuffer() throws IOException{
        // create zip file bytes from buffer store and write to underlying store
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

//        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
//            // iterate over bufferStore.list()
//            for (String entry: bufferStore.list(new String[]{}).toArray(String[]::new)) {
//                List<String> pathComponents = entry.getKey();
//                byte[] data = entry.getValue();
//
//                // Build the ZIP path (e.g. ["dir", "sub", "file.txt"] → "dir/sub/file.txt")
//                String path = String.join("/", pathComponents);
//
//                ZipEntry zipEntry = new ZipEntry(path);
//                zos.putNextEntry(zipEntry);
//
//                zos.write(data);
//                zos.closeEntry();
//            }
//        }

//        byte[] zipBytes = baos.toByteArray();
//        return ByteBuffer.wrap(zipBytes);
//
//        underlyingStore.set();
    }

    private void loadBuffer() throws IOException{
        // read zip file bytes from underlying store and populate buffer store
        ByteBuffer buffer = underlyingStore.read();
        if (buffer == null) {
            return;
        }
        try (ZipInputStream zis = new ZipInputStream(new ByteBufferBackedInputStream(buffer))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    zis.closeEntry();
                    continue;
                }
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                byte[] tmp = new byte[8192];
                int read;
                while ((read = zis.read(tmp)) != -1) {
                    baos.write(tmp, 0, read);
                }

                byte[] bytes = baos.toByteArray();
                System.out.println("Loading entry: " + entry.getName() + " (" + bytes.length + " bytes)");

                bufferStore.set(new String[]{entry.getName()}, ByteBuffer.wrap(bytes));

                zis.closeEntry();
            }
        }

    }

    public BufferedZipStore(@Nonnull StoreHandle underlyingStore, @Nonnull Store.ListableStore bufferStore) {
        this.underlyingStore = underlyingStore;
        this.bufferStore = bufferStore;
        try {
            loadBuffer();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load buffer from underlying store", e);
        }
    }

    public BufferedZipStore(@Nonnull StoreHandle underlyingStore) {
        this(underlyingStore, new MemoryStore());
    }

    public BufferedZipStore(@Nonnull Path underlyingStore) {
        this(new FilesystemStore(underlyingStore.getParent()).resolve(underlyingStore.getFileName().toString()));
        System.out.println("Created BufferedZipStore with underlying path: " + this.underlyingStore.toString());

    }

    public BufferedZipStore(@Nonnull String underlyingStorePath) {
        this(Paths.get(underlyingStorePath));
    }

    /**
     *  Flushes the buffer to the underlying store as a zip file.
     */
    public void flush() throws IOException {
        writeBuffer();
    }

    @Override
    public Stream<String[]> list(String[] keys) {
        return bufferStore.list(keys);
    }

    @Override
    public boolean exists(String[] keys) {
        return bufferStore.exists(keys);
    }

    @Nullable
    @Override
    public ByteBuffer get(String[] keys) {
        return bufferStore.get(keys);
    }

    @Nullable
    @Override
    public ByteBuffer get(String[] keys, long start) {
        return bufferStore.get(keys, start);
    }

    @Nullable
    @Override
    public ByteBuffer get(String[] keys, long start, long end) {
        return bufferStore.get(keys, start, end);
    }

    @Override
    public void set(String[] keys, ByteBuffer bytes) {
        bufferStore.set(keys, bytes);
    }

    @Override
    public void delete(String[] keys) {
        bufferStore.delete(keys);
    }

    @Nonnull
    @Override
    public StoreHandle resolve(String... keys) {
        return new StoreHandle(this, keys);
    }

    @Override
    public String toString() {
        return "BufferedZipStore(" + underlyingStore.toString() + ")";
    }

    static class ByteBufferBackedInputStream extends InputStream {
        private final ByteBuffer buf;

        public ByteBufferBackedInputStream(ByteBuffer buf) {
            this.buf = buf;
        }

        @Override
        public int read() {
            return buf.hasRemaining() ? (buf.get() & 0xFF) : -1;
        }

        @Override
        public int read(byte[] bytes, int off, int len) {
            if (!buf.hasRemaining()) {
                return -1;
            }

            int toRead = Math.min(len, buf.remaining());
            buf.get(bytes, off, toRead);
            return toRead;
        }
    }

}
