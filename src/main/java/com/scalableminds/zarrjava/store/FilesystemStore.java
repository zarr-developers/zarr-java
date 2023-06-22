package com.scalableminds.zarrjava.store;

import com.scalableminds.zarrjava.v3.Utils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.util.Iterator;
import java.util.stream.Stream;

public class FilesystemStore implements Store, Store.ListableStore {

    @Nonnull
    private final FileSystem fileSystem;
    @Nonnull
    private final String path;

    public FilesystemStore(@Nonnull String path) {
        this.fileSystem = FileSystems.getDefault();
        this.path = path;
    }

    @Nullable
    @Override
    public ByteBuffer get(String key) {
        Path keyPath = fileSystem.getPath(this.path, key);
        try {
            ByteBuffer bytes = ByteBuffer.wrap(Files.readAllBytes(keyPath));
            return bytes;
        } catch (IOException e) {
            return null;
        }
    }

    @Nullable
    @Override
    public ByteBuffer get(String key, long start) {
        Path keyPath = fileSystem.getPath(this.path, key);

        try (SeekableByteChannel byteChannel = Files.newByteChannel(keyPath)) {
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
    public ByteBuffer get(String key, long start, long end) {
        Path keyPath = fileSystem.getPath(this.path, key);

        try (SeekableByteChannel byteChannel = Files.newByteChannel(keyPath)) {
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
    public void set(String key, ByteBuffer bytes) {
        Path keyPath = fileSystem.getPath(this.path, key);
        try {
            Files.createDirectories(keyPath.getParent());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try (SeekableByteChannel channel = Files.newByteChannel(keyPath, StandardOpenOption.WRITE)) {
            channel.write(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(String key) {
        Path keyPath = fileSystem.getPath(this.path, key);
        try {
            Files.delete(keyPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Iterator<String> list(String key) {
        Path keyPath = fileSystem.getPath(this.path, key);
        try (Stream<Path> paths = Files.list(keyPath)) {
            return paths.map(p -> p.toFile().getName()).iterator();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return fileSystem.getPath(this.path).toUri().toString().replaceAll("\\/$", "");
    }

}
