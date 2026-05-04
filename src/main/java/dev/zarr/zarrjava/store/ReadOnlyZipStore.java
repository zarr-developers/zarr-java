package dev.zarr.zarrjava.store;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.Enumeration;
import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.Collections;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * A Store implementation that provides read-only access to a zip archive stored in an underlying Store.
 * Compared to BufferedZipStore, this implementation reads directly from the zip archive without parsing
 * its contents into a buffer store first making it more efficient for read-only access to large zip archives.
 */
public class ReadOnlyZipStore extends ZipStore {
    private static final Logger logger = Logger.getLogger(ReadOnlyZipStore.class.getName());
    private final Path zipStorePath; // Store the resolved zip path for logging and potential future use
    // Cache keys are without trailing or leading slashes, leaf nodes are stored as their names
    private Map<String, Set<String>> directoryToChildrenDirectoriesIndex;
    private Map<String, Set<String>> directoryToChildrenFilesIndex;
    private Map<String, Long> fileSizeIndex;
    private boolean isCached = false;


    // Main constructor: receives a StoreHandle directly
    public ReadOnlyZipStore(@Nonnull StoreHandle handle) {
        super(handle);
        zipStorePath = underlyingStore.toPath(); // throws if not FilesystemStore
        String msg = String.format("New instance dev.zarr.zarrjava.store.ReadOnlyZipStore with path: %s",
                zipStorePath.toString());
        logger.log(Level.INFO, msg);
    }

    // Convenience constructor for filesystem paths
    public ReadOnlyZipStore(@Nonnull Path zipPath) {
        this(new FilesystemStore(zipPath.getParent()).resolve(
                zipPath.getFileName().toString()));
    }

    // Convenience constructor for string paths
    public ReadOnlyZipStore(@Nonnull String zipPath) {
        this(Paths.get(zipPath));
    }


    // Helper for buildZipIndex to add a file entry to the file index and ensure all its parent directories are indexed as well
    private void insertLeafEntry(String entryStrippedPath, long size) {
        fileSizeIndex.put(entryStrippedPath, size); // Cache the file size for quick access	
        int lastSlash = entryStrippedPath.lastIndexOf('/'); // Find the last '/' in the file name returns -1 if not found
        if (lastSlash >= 0) {
            String name = entryStrippedPath.substring(lastSlash + 1); // Extract the file name after the last slash
            String parentDir = entryStrippedPath.substring(0, lastSlash); // Extract the parent directory path without trailing slash
            directoryToChildrenFilesIndex.computeIfAbsent(parentDir,
                    k -> ConcurrentHashMap.newKeySet()).add(name); // Add the file name to the parent directory's set of children files
            insertDirectoryEntry(parentDir); // Recursively ensure all parent directories are added

        } else {// no slashes, file is in root directory
            directoryToChildrenFilesIndex.computeIfAbsent("",
                    k -> ConcurrentHashMap.newKeySet()).add(entryStrippedPath);
        }
    }

    // Helper for buildZipIndex to add a directory entry to the directory index and ensure all its parent directories are indexed as well
    private void insertDirectoryEntry(String entryStrippedPath) {
        int lastSlash = entryStrippedPath.lastIndexOf('/'); // Find the last '/' in the file name returns -1 if not found
        if (lastSlash >= 0) {
            String name = entryStrippedPath.substring(lastSlash + 1); // Extract the file name after the last slash
            String parentDir = entryStrippedPath.substring(0, lastSlash); // Extract the parent directory path without trailing slash
            directoryToChildrenDirectoriesIndex.computeIfAbsent(parentDir,
                    k -> ConcurrentHashMap.newKeySet()).add(name); // Add the file name to the parent directory's set of children files
            insertDirectoryEntry(parentDir); // Recursively ensure all parent directories are added

        } else {// no slashes, file is in root directory
            // No parent directory, just add the file name to the root directory index
            directoryToChildrenDirectoriesIndex.computeIfAbsent("",
                    k -> ConcurrentHashMap.newKeySet()).add(entryStrippedPath);
        }
    }

    private synchronized void buildZipIndex() {
        // Fast path using ZipFile
        try (ZipFile zf = new ZipFile(zipStorePath.toFile())) {
            // Optionally pre-size: count entries once (cheap and avoids rehashing on huge zips)
            int entryCount = zf.size();
            fileSizeIndex = new ConcurrentHashMap<>(Math.max(16, entryCount), 0.75f);
            directoryToChildrenDirectoriesIndex = new ConcurrentHashMap<>(Math.max(16, entryCount / 2), 0.75f);
            directoryToChildrenFilesIndex = new ConcurrentHashMap<>(Math.max(16, entryCount / 2), 0.75f);

            Enumeration<? extends ZipEntry> en = zf.entries();
            while (en.hasMoreElements()) {
                ZipEntry e = en.nextElement();
                String entryStrippedPath = normalizeEntryName(e.getName());
                if (entryStrippedPath.isEmpty()) continue; // guard against odd entries

                if (e.isDirectory() || entryStrippedPath.endsWith("/")) {
                    // Ensure directory entryStrippedPaths end with '/' ... this just complicates
                    if (entryStrippedPath.endsWith("/")) {
                        entryStrippedPath = entryStrippedPath.substring(0, entryStrippedPath.length() - 1);
                        logger.log(Level.WARNING,
                                "Directory entry '{0}' did end with '/' not removed by normalizeEntryName()",
                                e.getName());
                    } else if (entryStrippedPath.startsWith("/")) {
                        entryStrippedPath = entryStrippedPath.substring(1);
                        logger.log(Level.WARNING,
                                "Directory entry '{0}' did start with '/' not removed by normalizeEntryName()",
                                e.getName());
                    }
                    directoryToChildrenDirectoriesIndex.computeIfAbsent(entryStrippedPath,
                            k -> ConcurrentHashMap.newKeySet());
                    directoryToChildrenFilesIndex.computeIfAbsent(entryStrippedPath,
                            k -> ConcurrentHashMap.newKeySet());
                    insertDirectoryEntry(entryStrippedPath);
                } else {
                    // Put file size (may be -1 for STORED anomalies, but ZipFile usually knows it)
                    long size = e.getSize();
                    insertLeafEntry(entryStrippedPath, size);
                }
            }
        } catch (IOException e) {
            throw StoreException.readFailed(
                    underlyingStore.toString(), new String[]{},
                    new IOException("Failed to read ZIP central directory from filesystem path", e)
            );
        }
    }

    private synchronized void ensureCache() {
        if (isCached) return;
        long startTime, endTime;
        startTime = System.currentTimeMillis();
        buildZipIndex();
        isCached = true;
        endTime = System.currentTimeMillis();
        logger.log(Level.FINE, "Indexed ZIP store {0} with {1} file entries in {2} ms", new Object[]{zipStorePath.toString(), fileSizeIndex.size(), (endTime - startTime)});
    }

    String resolveKeys(String[] keys) {
        return String.join("/", keys);
    }

    private String resolvePathWithLeadingSlashFromKeys(String[] keys) {
        // Join the keys with "/" and add a leading slash
        String path = "/" + String.join("/", keys);
        return path;
    }

    String[] resolveEntryKeys(String entryKey) {
        return entryKey.split("/");
    }

    @Override
    public boolean exists(String[] keys) {
        ensureCache();
        String key = resolveKeys(keys);
        return fileSizeIndex.containsKey(key);
        //This tests for existence of files not directories by design for testing dirs and files use the following line
        //return fileSizeIndex.containsKey(key) || directoryToChildrenDirectoriesIndex.containsKey(key);
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

    //Helper function for get and getInputStream not to duplicate code
    private byte[] readEntryBytes(String[] keys, long start, long end) {
        ensureCache();
        String key = resolveKeys(keys);
        Long entrySize = fileSizeIndex.get(key);// use cached size not calling entry.getSize()
        if (entrySize == null) // key is not present in index
        {
            return null;
        }

        try (ZipFile zf = new ZipFile(zipStorePath.toFile())) {
            ZipEntry entry = zf.getEntry(key);
            if (entry == null) {
                String pathInZip = resolvePathWithLeadingSlashFromKeys(keys);// Sometimes paths in zip start with leading slash
                entry = zf.getEntry(pathInZip);
            }
            if (entry == null || entry.isDirectory()) {
                return null;
            }
            if (start < 0 || end < -1 || (end != -1 && end <= start)) {
                String msg = String.format("Invalid byte range [%d, %d) for entry '%s', returning empty array",
                        start, end, key);
                logger.log(Level.WARNING, msg);
                return new byte[0];
            }
            if (entrySize >= 0) {
                if (start >= entrySize || (end != -1 && end > entrySize)) {
                    String msg = String.format(
                            "Requested byte range [%d, %d) is out of bounds for entry '%s' with size %d, returning empty array",
                            start, end, key, entrySize);
                    logger.log(Level.WARNING, msg);
                    return new byte[0];
                }
            }

            try (InputStream is = zf.getInputStream(entry)) {
                //Skip to start position in the entry
                long toSkip = start;
                while (toSkip > 0) {
                    long skipped = is.skip(toSkip);
                    if (skipped <= 0) {
                        // fallback: read and discard
                        if (is.read() == -1) {
                            throw new IOException("Unexpected EOF while skipping to " + start);
                        }
                        skipped = 1;
                    }
                    toSkip -= skipped;
                }

                // Create ByteBuffer for the requested range
                long bytesToRead;
                if (end != -1) {
                    bytesToRead = end - start;
                } else if (entrySize >= 0) {
                    bytesToRead = entrySize - start;
                } else {
                    bytesToRead = Long.MAX_VALUE; // read until EOF if size is unknown and no end specified
                }

                ByteArrayOutputStream baos = new ByteArrayOutputStream(bytesToRead > 0 && bytesToRead < Integer.MAX_VALUE ? (int) bytesToRead : 8192);
                byte[] bufferArray = new byte[8192];
                int readLength = 0;
                while (bytesToRead > 0 && (readLength = is.read(bufferArray, 0, (int) Math.min(bufferArray.length,
                        bytesToRead))) != -1) {
                    baos.write(bufferArray, 0, readLength);
                    bytesToRead -= readLength;
                }
                return baos.toByteArray();
            }
        } catch (IOException io) {
            throw StoreException.readFailed(
                    underlyingStore.toString(), keys,
                    new IOException("Failed to read ZIP central directory from filesystem path", io)
            );
        }
    }


    @Nullable
    @Override
    public ByteBuffer get(String[] keys, long start, long end) {
        byte[] bytes = readEntryBytes(keys, start, end);
        if (bytes == null) {
            return null;
        }
        return ByteBuffer.wrap(bytes);
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

    private static String[] concatPaths(String[] prefix, String[] child) {
        String[] result = new String[prefix.length + child.length];
        System.arraycopy(prefix, 0, result, 0, prefix.length);
        System.arraycopy(child, 0, result, prefix.length, child.length);
        return result;
    }


    private void addChildrenRecursively(String[] prefixZarrPath, String[] childrenZarrPath, Stream.Builder<String[]> builder) {
        String[] fullZarrPath = concatPaths(prefixZarrPath, childrenZarrPath);

        String prefix = resolveKeys(fullZarrPath);
        Set<String> childrenDir = directoryToChildrenDirectoriesIndex.get(prefix);
        Set<String> childrenFile = directoryToChildrenFilesIndex.get(prefix);

        if (childrenFile != null) {
            for (String child : childrenFile) {
                builder.add(concatPaths(childrenZarrPath, new String[]{child}));
            }
        }

        if (childrenDir != null) {
            for (String child : childrenDir) {
                addChildrenRecursively(prefixZarrPath, concatPaths(childrenZarrPath, new String[]{child}), builder);
            }
        }
    }

    // Returns all descendant files and in string avoid prefixZarrPath
    @Override
    public Stream<String[]> list(String[] prefixZarrPath) {
        ensureCache();
        String prefix = resolveKeys(prefixZarrPath);

        Set<String> childrenDir = directoryToChildrenDirectoriesIndex.get(prefix);
        Set<String> childrenFile = directoryToChildrenFilesIndex.get(prefix);

        Stream.Builder<String[]> builder = Stream.builder();

        if (childrenFile != null) {
            for (String child : childrenFile) {
                String[] childrenZarrPath = new String[]{child};
                builder.add(childrenZarrPath);
            }
        }
        if (childrenDir != null) {
            for (String child : childrenDir) {
                String[] childrenZarrPath = new String[]{child};
                addChildrenRecursively(prefixZarrPath, childrenZarrPath, builder);
            }
        }

        return builder.build();
    }

    //Returns both file and directory children of the given prefix path, but only one level deep (no recursion)
    @Override
    public Stream<String> listChildren(String[] prefixKeys) {
        ensureCache();

        String prefix = resolveKeys(prefixKeys);

        Set<String> childrenDir = directoryToChildrenDirectoriesIndex.get(prefix);
        Set<String> childrenFile = directoryToChildrenFilesIndex.get(prefix);

        // If either set is null, treat it as an empty set
        Stream<String> dirStream = (childrenDir != null) ? childrenDir.stream() : Stream.empty();
        Stream<String> fileStream = (childrenFile != null) ? childrenFile.stream() : Stream.empty();

        // Concatenate the streams and return the result
        return Stream.concat(dirStream, fileStream);
    }


    // Normalize entry names to ensure consistent handling of leading/trailing slashes
    // Name of root directory will then be ""
    private String normalizeEntryName(String name) {
        if (name.startsWith("/")) name = name.substring(1);
        if (name.endsWith("/")) name = name.substring(0, name.length() - 1);
        return name;
    }


    @Override
    public InputStream getInputStream(String[] keys, long start, long end) {
        byte[] bytes = readEntryBytes(keys, start, end);
        if (bytes == null) {
            return null;
        }
        return new ByteArrayInputStream(bytes);
    }

    @Override
    public long getSize(String[] keys) {
        ensureCache();
        String key = resolveKeys(keys);
        Long cachedSize = fileSizeIndex.get(key);
        if (cachedSize == null) {
            return -1;
        }
        if (cachedSize >= 0) {
            return cachedSize;
        }

        // if size is not in header/cache, we fallback to reading the entry to determine size (inefficient and probably required only for some zip anomalies)
        try (ZipFile zf = new ZipFile(zipStorePath.toFile())) {
            ZipEntry entry = zf.getEntry(key);
            if (entry == null) {
                String pathInZip = resolvePathWithLeadingSlashFromKeys(keys);// Sometimes paths in zip start with leading slash
                logger.log(Level.WARNING, "ZipEntry is null for key: {0} when attempting to get size, trying with leading slash", key);
                entry = zf.getEntry(pathInZip);
            }
            if (entry == null || entry.isDirectory()) {
                return -1;
            }
            long entrySize = entry.getSize();// may be -1 for STORED entries without proper headers, but ZipFile usually handles this
            return entrySize;
        } catch (IOException e) {
            throw StoreException.readFailed(
                    underlyingStore.toString(), keys,
                    new IOException("Failed to read ZIP central directory from filesystem path", e)
            );
        }
    }
}
