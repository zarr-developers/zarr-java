package dev.zarr.zarrjava.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.Utils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;
import static dev.zarr.zarrjava.v2.Node.makeObjectMapper;

public class Group extends dev.zarr.zarrjava.core.Group implements Node{
  public GroupMetadata metadata;

  protected Group(@Nonnull StoreHandle storeHandle, @Nonnull GroupMetadata groupMetadata) throws IOException {
    super(storeHandle);
    this.metadata = groupMetadata;
  }

  public static Group open(@Nonnull StoreHandle storeHandle) throws IOException {
    StoreHandle metadataHandle = storeHandle.resolve(ZGROUP);
    ByteBuffer metadataBytes = metadataHandle.readNonNull();
    return new Group(storeHandle, makeObjectMapper()
        .readValue(Utils.toArray(metadataBytes), GroupMetadata.class));
  }

  public static Group open(Path path) throws IOException {
      return open(new StoreHandle(new FilesystemStore(path)));
    }

    public static Group open(String path) throws IOException {
      return open(Paths.get(path));
    }

  public static Group create(
      @Nonnull StoreHandle storeHandle, @Nonnull GroupMetadata groupMetadata
  ) throws IOException {
    ObjectMapper objectMapper = makeObjectMapper();
    ByteBuffer metadataBytes = ByteBuffer.wrap(objectMapper.writeValueAsBytes(groupMetadata));
    storeHandle.resolve(ZGROUP).set(metadataBytes);
    return new Group(storeHandle, groupMetadata);
  }

  public static Group create(@Nonnull StoreHandle storeHandle) throws IOException, ZarrException {
    return create(storeHandle, new GroupMetadata());
  }

  public static Group create(Path path) throws IOException, ZarrException {
    return create(new StoreHandle(new FilesystemStore(path)));
  }

  public static Group create(String path) throws IOException, ZarrException {
    return create(Paths.get(path));
  }

  @Nullable
  public Node get(String key) throws ZarrException {
    StoreHandle keyHandle = storeHandle.resolve(key);
    try {
      return Node.open(keyHandle);
    } catch (IOException e) {
        return null;
    }
  }

  public Group createGroup(String key) throws IOException, ZarrException {
    return Group.create(storeHandle.resolve(key));
  }

  public Array createArray(String key, ArrayMetadata arrayMetadata)
      throws IOException, ZarrException {
    return Array.create(storeHandle.resolve(key), arrayMetadata);
  }

  public Array createArray(String key, Function<ArrayMetadataBuilder, ArrayMetadataBuilder> arrayMetadataBuilderMapper)
      throws IOException, ZarrException {
    return Array.create(storeHandle.resolve(key), arrayMetadataBuilderMapper, false);
  }

  @Override
  public String toString() {
    return String.format("<v2.Group {%s}>", storeHandle);
  }
}
