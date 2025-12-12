package dev.zarr.zarrjava.store;

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static dev.zarr.zarrjava.utils.ZipUtils.getZipCommentFromBuffer;

public class ReadOnlyZipStore implements Store, Store.ListableStore {

    private final StoreHandle underlyingStore;

    String resolveKeys(String[] keys) {
        return String.join("/", keys);
    }

    String[] resolveEntryKeys(String entryKey) {
        return entryKey.split("/");
    }

    @Override
    public boolean exists(String[] keys) {
        return get(keys, 0, 0) != null;
    }

    @Nullable
    @Override
    public ByteBuffer get(String[] keys) {
        return get(keys, 0);
    }

    @Nullable
    @Override
    public ByteBuffer get(String[] keys, long start) {
        return get(keys, start, -1);
    }

    public String getArchiveComment() throws IOException {
        ByteBuffer buffer = underlyingStore.read();
        if (buffer == null) {
            return null;
        }
        byte[] bufArray;
        if (buffer.hasArray()) {
            bufArray = buffer.array();
        } else {
            bufArray = new byte[buffer.remaining()];
            buffer.duplicate().get(bufArray);
        }
        return getZipCommentFromBuffer(bufArray);
    }

    @Nullable
    @Override
    public ByteBuffer get(String[] keys, long start, long end) {
        ByteBuffer buffer = underlyingStore.read();
        if (buffer == null) {
            return null;
        }
        try (ZipArchiveInputStream zis = new ZipArchiveInputStream(new ByteBufferBackedInputStream(buffer))) {
            ZipArchiveEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (entry.isDirectory() || !entry.getName().equals(resolveKeys(keys))) {
                    continue;
                }
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                if (end == -1) {
                    end = entry.getSize();
                }
                if (start > end) {
                    throw new IllegalArgumentException("Start position can not be larger than end position. Got start=" + start + ", end=" + end);
                }
                if (start < 0 || end > entry.getSize()) {
                    throw new IllegalArgumentException("Start and end positions must be within the bounds of the zip entry size. Entry size=" + entry.getSize() + ", got start=" + start + ", end=" + end);
                }
                zis.skip(start);
                long bytesToRead = end - start;
                byte[] bufferArray = new byte[8192];
                int len;
                while (bytesToRead > 0 && (len = zis.read(bufferArray, 0, (int) Math.min(bufferArray.length, bytesToRead))) != -1) {
                    baos.write(bufferArray, 0, len);
                    bytesToRead -= len;
                }
                byte[] bytes = baos.toByteArray();
                return ByteBuffer.wrap(bytes);
            }
        } catch (IOException e) {
            return null;
        }
        return null;
    }

    @Override
    public void set(String[] keys, ByteBuffer bytes) {
        throw new UnsupportedOperationException("ReadOnlyZipStore does not support set operation.");
    }

    @Override
    public void delete(String[] keys) {
        throw new UnsupportedOperationException("ReadOnlyZipStore does not support delete operation.");
    }

    @Nonnull
    @Override
    public StoreHandle resolve(String... keys) {
        return new StoreHandle(this, keys);
    }

    @Override
    public String toString() {
        return "ReadOnlyZipStore(" + underlyingStore.toString() + ")";
    }

    public ReadOnlyZipStore(@Nonnull StoreHandle underlyingStore) {
        this.underlyingStore = underlyingStore;
    }

    public ReadOnlyZipStore(@Nonnull Path underlyingStore) {
        this(new FilesystemStore(underlyingStore.getParent()).resolve(underlyingStore.getFileName().toString()));
    }

    public ReadOnlyZipStore(@Nonnull String underlyingStorePath) {
        this(Paths.get(underlyingStorePath));
    }

    @Override
    public Stream<String[]> list(String[] keys) {
        Stream.Builder<String[]> builder = Stream.builder();

        ByteBuffer buffer = underlyingStore.read();
        if (buffer == null) {
            return builder.build();
        }
        try (ZipArchiveInputStream zis = new ZipArchiveInputStream(new ByteBufferBackedInputStream(buffer))) {
            ZipArchiveEntry entry;
            String prefix = resolveKeys(keys);
            while ((entry = zis.getNextEntry()) != null) {
                String entryKey = entry.getName();
                if (!entryKey.startsWith(prefix) || entryKey.equals(prefix)) {
                    continue;
                }
                String[] entryKeys = resolveEntryKeys(entryKey.substring(prefix.length()));
                builder.add(entryKeys);
            }
        } catch (IOException e) {
            return null;
        }
        return builder.build();
    }
}
