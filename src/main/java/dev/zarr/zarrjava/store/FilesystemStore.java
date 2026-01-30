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

public class FilesystemStore implements Store, Store.ListableStore {

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

        try {
            // Use toRealPath() to resolve symlinks and verify path is within root
            // For non-existent paths, validate the existing parent path
            Path absoluteRoot = path.toAbsolutePath().normalize();
            Path targetPath = newPath.toAbsolutePath().normalize();

            // Try to get real path if it exists (follows symlinks)
            if (Files.exists(targetPath)) {
                Path realTarget = targetPath.toRealPath();
                Path realRoot = absoluteRoot.toRealPath();
                if (!realTarget.startsWith(realRoot)) {
                    throw new IllegalArgumentException("Key resolves outside of store root: " + realTarget);
                }
            } else {
                // For non-existent paths, check the normalized path
                // and ensure existing parent doesn't escape via symlinks
                Path parent = targetPath.getParent();
                if (parent != null && Files.exists(parent)) {
                    Path realParent = parent.toRealPath();
                    Path realRoot = absoluteRoot.toRealPath();
                    if (!realParent.startsWith(realRoot)) {
                        throw new IllegalArgumentException("Parent path resolves outside of store root: " + realParent);
                    }
                } else if (!targetPath.startsWith(absoluteRoot)) {
                    throw new IllegalArgumentException("Key resolves outside of store root: " + targetPath);
                }
            }
        } catch (IOException e) {
            // If toRealPath() fails, fall back to normalized path check
            Path absoluteRoot = path.toAbsolutePath().normalize();
            Path absoluteTarget = newPath.toAbsolutePath().normalize();
            if (!absoluteTarget.startsWith(absoluteRoot)) {
                throw new IllegalArgumentException("Key resolves outside of store root: " + absoluteTarget);
            }
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
        } catch (IOException e) {
            return null;
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
        } catch (IOException e) {
            return null;
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
        } catch (IOException e) {
            return null;
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

    /**
     * Helper to convert a filesystem Path back into the full String[] key array
     * relative to the prefix
     */
    private String[] pathToKeyArray(Path rootPath, Path currentPath, String[] prefix) {
        Path relativePath = rootPath.relativize(currentPath);
        int relativeCount = relativePath.getNameCount();

        String[] result = new String[relativeCount];
        for (int i = 0; i < relativeCount; i++) {
            result[i] = relativePath.getName(i).toString();
        }
        return result;
    }

    @Override
    public Stream<String[]> list(String[] prefix) {
        Path rootPath = resolveKeys(prefix);
        try {
            return Files.walk(rootPath)
                    .filter(Files::isRegularFile)
                    .map(path -> pathToKeyArray(rootPath, path, prefix));
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
            if (!Files.exists(keyPath)) {
                return null;
            }
            InputStream inputStream = Files.newInputStream(keyPath);
            if (start > 0) {
                long skipped = inputStream.skip(start);
                if (skipped < start) {
                    throw new IOException("Unable to skip to position " + start +
                            ", only skipped " + skipped + " bytes in file: " + keyPath);
                }
            }
            if (end != -1) {
                long bytesToRead = end - start;
                return new BoundedInputStream(inputStream, bytesToRead);
            } else {
                return inputStream;
            }
        } catch (IOException e) {
            throw StoreException.readFailed(
                    this.toString(),
                    keys,
                    new IOException("Failed to open input stream for file: " + keyPath +
                            " (start: " + start + ", end: " + end + ")", e));
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
