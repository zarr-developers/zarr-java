package dev.zarr.zarrjava.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.Utils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;
import static dev.zarr.zarrjava.v2.Node.makeObjectMapper;
import static dev.zarr.zarrjava.v2.Node.makeObjectWriter;

public class Group extends dev.zarr.zarrjava.core.Group implements Node{
  public GroupMetadata metadata;

  protected Group(@Nonnull StoreHandle storeHandle, @Nonnull GroupMetadata groupMetadata) throws IOException {
    super(storeHandle);
    this.metadata = groupMetadata;
  }

  public static Group open(@Nonnull StoreHandle storeHandle) throws IOException {
    ObjectMapper mapper = makeObjectMapper();
    GroupMetadata metadata = mapper.readValue(
        Utils.toArray(storeHandle.resolve(ZGROUP).readNonNull()),
        GroupMetadata.class
    );
    if (storeHandle.resolve(ZATTRS).exists())
      metadata.attributes = mapper.readValue(
          Utils.toArray(storeHandle.resolve(ZATTRS).readNonNull()),
          dev.zarr.zarrjava.core.Attributes.class
      );
    return new Group(storeHandle, metadata);
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
    return new Group(storeHandle, groupMetadata).writeMetadata();
  }

  public static Group create(@Nonnull StoreHandle storeHandle) throws IOException, ZarrException {
    return create(storeHandle, new GroupMetadata());
  }

  public static Group create(@Nonnull StoreHandle storeHandle, Attributes attributes) throws IOException, ZarrException {
    return create(storeHandle, new GroupMetadata(attributes));
  }

  public static Group create(Path path) throws IOException, ZarrException {
    return create(new StoreHandle(new FilesystemStore(path)));
  }

  public static Group create(Path path, Attributes attributes) throws IOException, ZarrException {
    return create(new StoreHandle(new FilesystemStore(path)), attributes);
  }

  public static Group create(String path) throws IOException, ZarrException {
    return create(Paths.get(path));
  }

  public static Group create(String path, Attributes attributes) throws IOException, ZarrException {
    return create(Paths.get(path), attributes);
  }

  @Nullable
  public Node get(String key) throws ZarrException, IOException {
    StoreHandle keyHandle = storeHandle.resolve(key);
    try {
      return Node.open(keyHandle);
    } catch (NoSuchFileException e) {
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

  private Group writeMetadata() throws IOException {
    return writeMetadata(this.metadata);
  }

  private Group writeMetadata(GroupMetadata newGroupMetadata) throws IOException {
    ObjectWriter objectWriter = makeObjectWriter();
    ByteBuffer metadataBytes = ByteBuffer.wrap(objectWriter.writeValueAsBytes(newGroupMetadata));
    storeHandle.resolve(ZGROUP).set(metadataBytes);
    if (newGroupMetadata.attributes != null) {
      StoreHandle attrsHandle = storeHandle.resolve(ZATTRS);
      ByteBuffer attrsBytes = ByteBuffer.wrap(
          objectWriter.writeValueAsBytes(newGroupMetadata.attributes));
      attrsHandle.set(attrsBytes);
    }
    return new Group(storeHandle, newGroupMetadata);
  }

  public Group setAttributes(Attributes newAttributes) throws ZarrException, IOException {
    GroupMetadata newGroupMetadata = new GroupMetadata(newAttributes);
    return writeMetadata(newGroupMetadata);
  }

  public Group updateAttributes(Function<Attributes, Attributes> attributeMapper)
          throws ZarrException, IOException {
    return setAttributes(attributeMapper.apply(metadata.attributes));
  }


  @Override
  public String toString() {
    return String.format("<v2.Group {%s}>", storeHandle);
  }

  @Override
  public GroupMetadata metadata() {
    return metadata;
  }
}
