package dev.zarr.zarrjava.store;

import org.apache.commons.compress.archivers.zip.Zip64Mode;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;


/**
 * A Store implementation that buffers reads and writes and flushes them to an underlying Store as a zip file.
 */
public class BufferedZipStore extends ZipStore {

    private final Store.ListableStore bufferStore;
    private final boolean flushOnWrite;
    private final Comparator<String[]> zipEntryComparator = (a, b) -> {
        boolean aIsZarr = a.length > 0 && a[a.length - 1].equals("zarr.json");
        boolean bIsZarr = b.length > 0 && b[b.length - 1].equals("zarr.json");
        // first all zarr.json files
        if (aIsZarr && !bIsZarr) {
            return -1;
        } else if (!aIsZarr && bIsZarr) {
            return 1;
        } else if (aIsZarr && bIsZarr) {
            // sort zarr.json in BFS order within same depth by lexicographical order
            if (a.length != b.length) {
                return Integer.compare(a.length, b.length);
            } else {
                return String.join("/", a).compareTo(String.join("/", b));
            }
        } else {
            // then all other files in lexicographical order
            return String.join("/", a).compareTo(String.join("/", b));
        }
    };
    private String archiveComment;

    public BufferedZipStore(@Nonnull StoreHandle underlyingStore, @Nonnull Store.ListableStore bufferStore, @Nullable String archiveComment, boolean flushOnWrite) {
        super(underlyingStore);
        this.bufferStore = bufferStore;
        this.archiveComment = archiveComment;
        this.flushOnWrite = flushOnWrite;
        try {
            loadBuffer();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load buffer from underlying store", e);
        }
    }

    public BufferedZipStore(@Nonnull StoreHandle underlyingStore, @Nonnull Store.ListableStore bufferStore, @Nullable String archiveComment) {
        this(underlyingStore, bufferStore, archiveComment, false);
    }

    public BufferedZipStore(@Nonnull StoreHandle underlyingStore, @Nonnull Store.ListableStore bufferStore) {
        this(underlyingStore, bufferStore, null);
    }

    public BufferedZipStore(@Nonnull StoreHandle underlyingStore, String archiveComment) {
        this(underlyingStore, new MemoryStore(), archiveComment);
    }

    public BufferedZipStore(@Nonnull StoreHandle underlyingStore) {
        this(underlyingStore, (String) null);
    }

    public BufferedZipStore(@Nonnull Path underlyingStore, String archiveComment) {
        this(new FilesystemStore(underlyingStore.getParent()).resolve(underlyingStore.getFileName().toString()), archiveComment);
    }

    public BufferedZipStore(@Nonnull Path underlyingStore) {
        this(underlyingStore, null);
    }

    public BufferedZipStore(@Nonnull String underlyingStorePath, String archiveComment) {
        this(Paths.get(underlyingStorePath), archiveComment);
    }

    public BufferedZipStore(@Nonnull String underlyingStorePath) {
        this(underlyingStorePath, null);
    }

    public BufferedZipStore(@Nonnull StoreHandle underlyingStore, @Nonnull Store.ListableStore bufferStore, boolean flushOnWrite) {
        this(underlyingStore, bufferStore, null, flushOnWrite);
    }

    public BufferedZipStore(@Nonnull StoreHandle underlyingStore, String archiveComment, boolean flushOnWrite) {
        this(underlyingStore, new MemoryStore(), archiveComment, flushOnWrite);
    }

    public BufferedZipStore(@Nonnull StoreHandle underlyingStore, boolean flushOnWrite) {
        this(underlyingStore, (String) null, flushOnWrite);
    }

    public BufferedZipStore(@Nonnull Path underlyingStore, String archiveComment, boolean flushOnWrite) {
        this(new FilesystemStore(underlyingStore.getParent()).resolve(underlyingStore.getFileName().toString()), archiveComment, flushOnWrite);
    }

    public BufferedZipStore(@Nonnull Path underlyingStore, boolean flushOnWrite) {
        this(underlyingStore, null, flushOnWrite);
    }

    public BufferedZipStore(@Nonnull String underlyingStorePath, String archiveComment, boolean flushOnWrite) {
        this(Paths.get(underlyingStorePath), archiveComment, flushOnWrite);
    }

    public BufferedZipStore(@Nonnull String underlyingStorePath, boolean flushOnWrite) {
        this(underlyingStorePath, null, flushOnWrite);
    }

    private void writeBuffer() throws IOException {
        // create zip file bytes from buffer store and write to underlying store
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipArchiveOutputStream zos = new ZipArchiveOutputStream(baos)) {
            zos.setUseZip64(Zip64Mode.AsNeeded);
            if (archiveComment != null) {
                zos.setComment(archiveComment);
            }
            bufferStore.list().sorted(zipEntryComparator).forEach(keys -> {
                try {
                    if (keys == null || keys.length == 0) {
                        // skip root entry
                        return;
                    }
                    String entryName = String.join("/", keys);
                    ByteBuffer bb = bufferStore.get(keys);
                    if (bb == null) {
                        // directory entry: ensure trailing slash
                        if (!entryName.endsWith("/")) {
                            entryName = entryName + "/";
                        }
                        ZipArchiveEntry dirEntry = new ZipArchiveEntry(entryName);
                        dirEntry.setMethod(ZipEntry.STORED);
                        dirEntry.setSize(0);
                        dirEntry.setCrc(0);
                        zos.putArchiveEntry(dirEntry);
                        zos.closeArchiveEntry();
                    } else {
                        // read bytes from ByteBuffer without modifying original
                        ByteBuffer dup = bb.duplicate();
                        int len = dup.remaining();
                        byte[] bytes = new byte[len];
                        dup.get(bytes);

                        // compute CRC and set size for STORED (no compression)
                        CRC32 crc = new CRC32();
                        crc.update(bytes, 0, bytes.length);
                        ZipArchiveEntry fileEntry = new ZipArchiveEntry(entryName);
                        fileEntry.setMethod(ZipEntry.STORED);
                        fileEntry.setSize(bytes.length);
                        fileEntry.setCrc(crc.getValue());

                        zos.putArchiveEntry(fileEntry);
                        zos.write(bytes);
                        zos.closeArchiveEntry();
                    }
                } catch (IOException e) {
                    // wrap checked exception so it can be rethrown from stream for handling below
                    throw new RuntimeException(e);
                }
            });
            zos.finish();
        } catch (RuntimeException e) {
            // unwrap and rethrow IOExceptions thrown inside the lambda
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw e;
        }

        byte[] zipBytes = baos.toByteArray();
        // write zip bytes back to underlying store
        underlyingStore.set(ByteBuffer.wrap(zipBytes));
    }

    public void deleteArchiveComment() throws IOException {
        this.setArchiveComment(null);
    }

    /**
     * Loads the buffer from the underlying store zip file.
     */
    private void loadBuffer() throws IOException {
        String loadedArchiveComment = super.getArchiveComment();
        if (loadedArchiveComment != null && this.archiveComment == null) {
            // don't overwrite existing archiveComment
            this.archiveComment = loadedArchiveComment;
        }

        InputStream inputStream = underlyingStore.getInputStream();
        if (inputStream == null) {
            return;
        }
        try (ZipArchiveInputStream zis = new ZipArchiveInputStream(inputStream)) {
            ZipArchiveEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    continue;
                }
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                byte[] tmp = new byte[8192];
                int read;
                while ((read = zis.read(tmp)) != -1) {
                    baos.write(tmp, 0, read);
                }
                byte[] bytes = baos.toByteArray();
                bufferStore.set(new String[]{entry.getName()}, ByteBuffer.wrap(bytes));
            }
        }
    }

    /**
     * Flushes the buffer and archiveComment to the underlying store as a zip file.
     */
    public void flush() throws IOException {
        writeBuffer();
    }

    @Override
    public String getArchiveComment() {
        return archiveComment;
    }

    public void setArchiveComment(@Nullable String archiveComment) throws IOException {
        this.archiveComment = archiveComment;
        if (flushOnWrite) {
            writeBuffer();
        }
    }

    @Override
    public Stream<String[]> list(String[] keys) {
        return bufferStore.list(keys);
    }

    @Override
    public Stream<String> listChildren(String[] prefix) {
        return bufferStore.listChildren(prefix);
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
        if (flushOnWrite) {
            try {
                writeBuffer();
            } catch (IOException e) {
                throw new RuntimeException("Failed to flush buffer to underlying store after set operation", e);
            }
        }
    }

    @Override
    public void delete(String[] keys) {
        bufferStore.delete(keys);
        if (flushOnWrite) {
            try {
                writeBuffer();
            } catch (IOException e) {
                throw new RuntimeException("Failed to flush buffer to underlying store after delete operation", e);
            }
        }
    }

    @Nonnull
    @Override
    public StoreHandle resolve(String... keys) {
        return new StoreHandle(this, keys);
    }

    @Override
    public InputStream getInputStream(String[] keys, long start, long end) {
        return bufferStore.getInputStream(keys, start, end);
    }

    public long getSize(String[] keys) {
        return bufferStore.getSize(keys);
    }

    @Override
    public String toString() {
        return "BufferedZipStore(" + underlyingStore.toString() + ")";
    }
}
