package dev.zarr.zarrjava.store;

import dev.zarr.zarrjava.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
    try {
      return Files.list(resolveKeys(keys)).map(path -> {
        Path relativePath = resolveKeys(keys).relativize(path);
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

}
