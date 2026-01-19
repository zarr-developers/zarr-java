package dev.zarr.zarrjava.store;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.io.input.BoundedInputStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Stream;


/**
 * A Store implementation that provides read-only access to a zip archive stored in an underlying Store.
 * Compared to BufferedZipStore, this implementation reads directly from the zip archive without parsing
 * its contents into a buffer store first making it more efficient for read-only access to large zip archives.
 */
public class ReadOnlyZipStore extends ZipStore {

    public ReadOnlyZipStore(@Nonnull StoreHandle underlyingStore) {
        super(underlyingStore);
    }

    public ReadOnlyZipStore(@Nonnull Path underlyingStore) {
        this(new FilesystemStore(underlyingStore.getParent()).resolve(underlyingStore.getFileName().toString()));
    }

    public ReadOnlyZipStore(@Nonnull String underlyingStorePath) {
        this(Paths.get(underlyingStorePath));
    }

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

    @Nullable
    @Override
    public ByteBuffer get(String[] keys, long start, long end) {
        InputStream inputStream = underlyingStore.getInputStream();
        if (inputStream == null) {
            return null;
        }
        try (ZipArchiveInputStream zis = new ZipArchiveInputStream(inputStream)) {
            ZipArchiveEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                String entryName = entry.getName();

                if (entryName.startsWith("/")) {
                    entryName = entryName.substring(1);
                }
                if (entry.isDirectory() || !entryName.equals(resolveKeys(keys))) {
                    continue;
                }

                long skipResult = zis.skip(start);
                if (skipResult != start) {
                    throw new IOException("Failed to skip to start position " + start + " in zip entry " + entryName);
                }

                long bytesToRead;
                if (end != -1) bytesToRead = end - start;
                else bytesToRead = Long.MAX_VALUE;

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
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

    @Override
    public Stream<String[]> list(String[] prefixKeys) {
        Stream.Builder<String[]> builder = Stream.builder();
        InputStream inputStream = underlyingStore.getInputStream();
        if (inputStream == null) return builder.build();

        String prefix = resolveKeys(prefixKeys);
        if (!prefix.isEmpty() && !prefix.endsWith("/")) {
            prefix += "/";
        }

        try (ZipArchiveInputStream zis = new ZipArchiveInputStream(inputStream)) {
            ZipArchiveEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                String name = normalizeEntryName(entry.getName());
                if (name.startsWith(prefix) && !entry.isDirectory()) {
                    builder.add(resolveEntryKeys(name.substring(prefix.length())));
                }
            }
        } catch (IOException ignored) {
        }
        return builder.build();
    }

    @Override
    public Stream<String> listChildren(String[] prefixKeys) {
        Set<String> children = new LinkedHashSet<>();
        InputStream inputStream = underlyingStore.getInputStream();
        if (inputStream == null) return Stream.empty();

        String prefix = resolveKeys(prefixKeys);
        if (!prefix.isEmpty() && !prefix.endsWith("/")) {
            prefix += "/";
        }

        try (ZipArchiveInputStream zis = new ZipArchiveInputStream(inputStream)) {
            ZipArchiveEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                String name = normalizeEntryName(entry.getName());

                if (name.startsWith(prefix) && !name.equals(prefix)) {
                    String relative = name.substring(prefix.length());
                    String[] parts = relative.split("/");
                    children.add(parts[0]);
                }
            }
        } catch (IOException ignored) {
        }

        return children.stream();
    }

    private String normalizeEntryName(String name) {
        if (name.startsWith("/")) name = name.substring(1);
        if (name.endsWith("/")) name = name.substring(0, name.length() - 1);
        return name;
    }


    @Override
    public InputStream getInputStream(String[] keys, long start, long end) {
        InputStream baseStream = underlyingStore.getInputStream();

        try {
            ZipArchiveInputStream zis = new ZipArchiveInputStream(baseStream);
            ZipArchiveEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                String entryName = entry.getName();

                if (entryName.startsWith("/")) {
                    entryName = entryName.substring(1);
                }
                if (entry.isDirectory() || !entryName.equals(resolveKeys(keys))) {
                    continue;
                }

                long skipResult = zis.skip(start);
                if (skipResult != start) {
                    throw new IOException("Failed to skip to start position " + start + " in zip entry " + entryName);
                }

                long bytesToRead;
                if (end != -1) bytesToRead = end - start;
                else bytesToRead = Long.MAX_VALUE;

                return new BoundedInputStream(zis, bytesToRead);
            }
            return null;
        } catch (IOException ignored) {
        }
        return null;
    }

    @Override
    public long getSize(String[] keys) {
        InputStream inputStream = underlyingStore.getInputStream();
        if (inputStream == null) {
            throw new RuntimeException(new IOException("Underlying store input stream is null"));
        }
        try (ZipArchiveInputStream zis = new ZipArchiveInputStream(inputStream)) {
            ZipArchiveEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                String entryName = entry.getName();

                if (entryName.startsWith("/")) {
                    entryName = entryName.substring(1);
                }
                if (entry.isDirectory() || !entryName.equals(resolveKeys(keys))) {
                    continue;
                }
                long size = entry.getSize();
                if (size < 0) {
                    // read the entire entry to determine size
                    size = 0;
                    byte[] bufferArray = new byte[8192];
                    int len;
                    while ((len = zis.read(bufferArray)) != -1) {
                        size += len;
                    }
                }
                return size;
            }
            return -1; // file not found
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
