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

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.zip.*;

import java.util.zip.CRC32;
import java.util.zip.ZipEntry; // for STORED constant


/** A Store implementation that buffers reads and writes and flushes them to an underlying Store as a zip file.
 */
public class BufferedZipStore implements Store, Store.ListableStore {

    private final StoreHandle underlyingStore;
    private final Store.ListableStore bufferStore;
    private String archiveComment;

    private void writeBuffer() throws IOException{
        // create zip file bytes from buffer store and write to underlying store
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipArchiveOutputStream zos = new ZipArchiveOutputStream(baos)) {
            zos.setUseZip64(Zip64Mode.AsNeeded);
            if (archiveComment != null) {
                zos.setComment(archiveComment);
            }
            Stream<String[]> entries = bufferStore.list().sorted(
                    (a, b) -> {
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
                    }
            );

            entries.forEach(keys -> {
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

    // Source - https://stackoverflow.com/a/9918966
    // Retrieved 2025-12-12, License - CC BY-SA 3.0
    private static String getZipCommentFromBuffer (byte[] buffer, int len) {
        byte[] magicDirEnd = {0x50, 0x4b, 0x05, 0x06};
        int buffLen = Math.min(buffer.length, len);

        // Check the buffer from the end
        for (int i = buffLen - magicDirEnd.length - 22; i >= 0; i--) {
            boolean isMagicStart = true;

            for (int k = 0; k < magicDirEnd.length; k++) {
                if (buffer[i + k] != magicDirEnd[k]) {
                    isMagicStart = false;
                    break;
                }
            }

            if (isMagicStart) {
                // Magic Start found!
                int commentLen = buffer[i + 20] + buffer[i + 21] * 256;
                int realLen = buffLen - i - 22;
                System.out.println ("ZIP comment found at buffer position "
                        + (i + 22) + " with len = " + commentLen + ", good!");

                if (commentLen != realLen) {
                    System.out.println ("WARNING! ZIP comment size mismatch: "
                            + "directory says len is " + commentLen
                            + ", but file ends after " + realLen + " bytes!");
                }

                String comment = new String (buffer, i + 22, Math.min(commentLen, realLen));
                return comment;
            }
        }

        System.out.println ("ERROR! ZIP comment NOT found!");
        return null;
    }

    private void loadBuffer() throws IOException{
        // read zip file bytes from underlying store and populate buffer store
        ByteBuffer buffer = underlyingStore.read();
        if (buffer == null) {
            return;
        }

        // read archive comment
        byte[] bufArray;
        if (buffer.hasArray()) {
            bufArray = buffer.array();
        } else {
            bufArray = new byte[buffer.remaining()];
            buffer.duplicate().get(bufArray);
        }
        this.archiveComment = getZipCommentFromBuffer(bufArray, bufArray.length);
        try (ZipArchiveInputStream zis = new ZipArchiveInputStream(new ByteBufferBackedInputStream(buffer))) {
            ArchiveEntry aentry;
            while ((aentry = zis.getNextEntry()) != null) {
                ZipArchiveEntry entry = (ZipArchiveEntry) aentry;
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

    public BufferedZipStore(@Nonnull StoreHandle underlyingStore, @Nonnull Store.ListableStore bufferStore, @Nullable String archiveComment) {
        this.underlyingStore = underlyingStore;
        this.bufferStore = bufferStore;
        this.archiveComment = archiveComment;
        try {
            loadBuffer();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load buffer from underlying store", e);
        }
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

    /**
     *  Flushes the buffer and archiveComment to the underlying store as a zip file.
     */
    public void flush() throws IOException {
        writeBuffer();
    }

    public String getArchiveComment() {
        return archiveComment;
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
