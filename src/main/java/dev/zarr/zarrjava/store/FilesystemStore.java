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
        return newPath;
    }

    @Override
    public boolean exists(String[] keys) {
        return Files.exists(resolveKeys(keys));
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
            throw new RuntimeException(e);
        }
        try (SeekableByteChannel channel = Files.newByteChannel(keyPath.toAbsolutePath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
        )) {
            channel.write(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(String[] keys) {
        try {
            Files.delete(resolveKeys(keys));
        } catch (NoSuchFileException e) {
            // ignore
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Stream<String[]> list(String[] keys) {
        Path keyPath = resolveKeys(keys);
        try {
            return Files.walk(keyPath)
                    .filter(path -> !path.equals(keyPath))
                    .map(path -> {
                        Path relativePath = keyPath.relativize(path);
                        String[] parts = new String[relativePath.getNameCount()];
                        for (int i = 0; i < relativePath.getNameCount(); i++) {
                            parts[i] = relativePath.getName(i).toString();
                        }
                        return parts;
                    });
        } catch (IOException e) {
            throw new RuntimeException(e);
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
                    throw new IOException("Unable to skip to the desired start position.");
                }
            }
            if (end != -1) {
                long bytesToRead = end - start;
                return new BoundedInputStream(inputStream, bytesToRead);
            } else {
                return inputStream;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public long getSize(String[] keys) {
        try {
            return Files.size(resolveKeys(keys));
        } catch (NoSuchFileException e) {
            return -1;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
