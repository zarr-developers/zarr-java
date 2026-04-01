package dev.zarr.zarrjava.store;

import dev.zarr.zarrjava.utils.Utils;
import org.apache.commons.io.input.BoundedInputStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.util.stream.Stream;
import java.io.File;
import java.util.stream.Collectors;
import java.util.stream.Stream.Builder;
import java.nio.file.attribute.BasicFileAttributes;
// Java logging
import java.util.logging.Logger;
import java.util.logging.Level;

public class FilesystemStore implements Store, Store.ListableStore {
    private static final Logger logger = Logger.getLogger(FilesystemStore.class.getName());

    @Nonnull
    private final Path path;

    public FilesystemStore(@Nonnull Path path) {
        this.path = path;
    }

    public FilesystemStore(@Nonnull String path) {
        this.path = Paths.get(path);
    }

    Path resolveKeys(String[] keys) {
        Path newPath = path;
        for (String key : keys) {
            newPath = newPath.resolve(key);
        }
        Path absRoot = path.toAbsolutePath().normalize();
        Path absTarget = newPath.toAbsolutePath().normalize();

        if (!absTarget.startsWith(absRoot)) {
            throw new IllegalArgumentException("Key resolves outside of store root: " + absTarget);
        }
        return newPath.normalize();
    }

    @Override
    public boolean exists(String[] keys) {
        return Files.isRegularFile(resolveKeys(keys));
    }

    @Nullable
    @Override
    public ByteBuffer get(String[] keys) {
        try {
            return ByteBuffer.wrap(Files.readAllBytes(resolveKeys(keys)));
        } catch (NoSuchFileException e) {
            return null;
        } catch (IOException e) {
            throw StoreException.readFailed(this.toString(), keys, e);
        }
    }

    @Nullable
    @Override
    public ByteBuffer get(String[] keys, long start) {
        try (SeekableByteChannel byteChannel = Files.newByteChannel(resolveKeys(keys))) {
            long startOffset = 0;
            if (start >= 0) {
                startOffset = start;
            } else {
                startOffset = byteChannel.size() + start;
            }
            long endOffset = byteChannel.size();
            ByteBuffer bytes = Utils.allocateNative((int) (endOffset - startOffset));
            byteChannel.position(startOffset);
            byteChannel.read(bytes);
            bytes.rewind();
            return bytes;
        } catch (NoSuchFileException e) {
            return null;
        } catch (IOException e) {
            throw StoreException.readFailed(this.toString(), keys, e);
        }
    }

    @Nullable
    @Override
    public ByteBuffer get(String[] keys, long start, long end) {
        try (SeekableByteChannel byteChannel = Files.newByteChannel(resolveKeys(keys))) {
            long startOffset = 0;
            if (start >= 0) {
                startOffset = start;
            } else {
                startOffset = byteChannel.size() + start;
            }
            ByteBuffer bytes = Utils.allocateNative((int) (end - startOffset));
            byteChannel.position(startOffset);
            byteChannel.read(bytes);
            bytes.rewind();
            return bytes;
        } catch (NoSuchFileException e) {
            return null;
        } catch (IOException e) {
            throw StoreException.readFailed(this.toString(), keys, e);
        }
    }


    @Override
    public void set(String[] keys, ByteBuffer bytes) {
        Path keyPath = resolveKeys(keys);
        try {
            Files.createDirectories(keyPath.getParent());
        } catch (IOException e) {
            throw StoreException.writeFailed(
                    this.toString(),
                    keys,
                    new IOException("Failed to create parent directories for path: " + keyPath.getParent(), e));
        }
        try (SeekableByteChannel channel = Files.newByteChannel(keyPath.toAbsolutePath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
        )) {
            channel.write(bytes);
        } catch (IOException e) {
            throw StoreException.writeFailed(
                    this.toString(),
                    keys,
                    new IOException("Failed to write " + bytes.remaining() + " bytes to file: " + keyPath, e));
        }
    }

    @Override
    public void delete(String[] keys) {
        Path keyPath = resolveKeys(keys);
        try {
            Files.delete(keyPath);
        } catch (NoSuchFileException e) {
            // ignore - file doesn't exist, which is the desired outcome
        } catch (IOException e) {
            throw StoreException.deleteFailed(
                    this.toString(),
                    keys,
                    new IOException("Failed to delete file: " + keyPath, e));
        }
    }

    @Override
    public Stream<String[]> list(String[] prefix) {
        Path rootPath = resolveKeys(prefix);
        try {
            Builder<String[]> builder = Stream.builder();  // Create a Stream.Builder to collect results
            // Walk the directory tree using walkFileTree
            Files.walkFileTree(rootPath, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                    // Only process regular files avoids Files::isRegularFile additional IO calls
                    if (attrs.isRegularFile()) {
                        String[] keys = rootPath.relativize(path).toString().split(File.separator);
                        builder.add(keys);  // Add the keys to the stream builder
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                    // Called when a file could not be visited
                    String msg = "Failed to visit file: " + file + " due to: " + exc.getMessage();
                    logger.log(Level.WARNING, msg, exc);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    return FileVisitResult.CONTINUE;
                }
            });

            return builder.build();  // Build the stream and return it

            //return Files.walk(rootPath).parallel().filter(Files::isRegularFile) // Filter only regular files (not directories)
            //             .map(path -> rootPath.relativize(path).toString().split(File.separator)); // Get relative path and split into keys
        } catch (IOException e) {
            throw StoreException.listFailed(
                    this.toString(),
                    prefix,
                    new IOException("Failed to walk directory tree at: " + rootPath, e));
        }
    }

    @Override
    public Stream<String> listChildren(String[] prefix) {
        Path rootPath = resolveKeys(prefix);
        if (!Files.exists(rootPath) || !Files.isDirectory(rootPath)) {
            return Stream.empty();
        }
        try {
            return Files.list(rootPath).map(path -> path.getFileName().toString());
        } catch (IOException e) {
            throw StoreException.listFailed(
                    this.toString(),
                    prefix,
                    new IOException("Failed to list directory contents at: " + rootPath, e));
        }
    }

    @Nonnull
    @Override
    public StoreHandle resolve(String... keys) {
        return new StoreHandle(this, keys);
    }

    @Override
    public String toString() {
        return this.path.toUri().toString().replaceAll("\\/$", "");
    }

    @Override
    public InputStream getInputStream(String[] keys, long start, long end) {
        Path keyPath = resolveKeys(keys);
        try {
            InputStream inputStream = Files.newInputStream(keyPath);
            if (start > 0) {
                long skipped = inputStream.skip(start);
                if (skipped < start) {
                    throw new IOException("Unable to skip to position " + start + ", only skipped " + skipped + " bytes in file: " + keyPath);
                }
            }
            if (end != -1) {
                long bytesToRead = end - start;
                return new BoundedInputStream(inputStream, bytesToRead);
            } else {
                return inputStream;
            }
        } catch (NoSuchFileException e) {
            return null;
        } catch (IOException e) {
            throw StoreException.readFailed(
                    this.toString(),
                    keys,
                    new IOException("Failed to open input stream for file: " + keyPath + " (start: " + start + ", end: " + end + ")", e));
        }
    }

    public long getSize(String[] keys) {
        Path keyPath = resolveKeys(keys);
        try {
            return Files.size(keyPath);
        } catch (NoSuchFileException e) {
            return -1;
        } catch (IOException e) {
            throw StoreException.readFailed(
                    this.toString(),
                    keys,
                    new IOException("Failed to get file size for: " + keyPath, e));
        }
    }
}
