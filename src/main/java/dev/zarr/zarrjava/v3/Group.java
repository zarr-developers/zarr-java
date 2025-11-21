package dev.zarr.zarrjava.v3;

import com.fasterxml.jackson.databind.ObjectWriter;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import static dev.zarr.zarrjava.v3.Node.makeObjectMapper;
import static dev.zarr.zarrjava.v3.Node.makeObjectWriter;


public class Group extends dev.zarr.zarrjava.core.Group implements Node {

  public GroupMetadata metadata;

  protected Group(@Nonnull StoreHandle storeHandle, @Nonnull GroupMetadata groupMetadata) throws IOException {
    super(storeHandle);
    this.metadata = groupMetadata;
  }

  public static Group open(@Nonnull StoreHandle storeHandle) throws IOException {
    StoreHandle metadataHandle = storeHandle.resolve(ZARR_JSON);
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
    ObjectWriter objectWriter = makeObjectWriter();
    ByteBuffer metadataBytes = ByteBuffer.wrap(objectWriter.writeValueAsBytes(groupMetadata));
    storeHandle.resolve(ZARR_JSON)
        .set(metadataBytes);
    return new Group(storeHandle, groupMetadata);
  }

  public static Group create(
      @Nonnull StoreHandle storeHandle,
      @Nonnull Attributes attributes
  ) throws IOException, ZarrException {
    return create(storeHandle, new GroupMetadata(attributes));
  }

  public static Group create(@Nonnull StoreHandle storeHandle) throws IOException, ZarrException {
    return create(storeHandle, GroupMetadata.defaultValue());
  }

  public static Group create(Path path, GroupMetadata groupMetadata) throws IOException, ZarrException {
    return create(new FilesystemStore(path).resolve(), groupMetadata);
  }

  public static Group create(String path, GroupMetadata groupMetadata) throws IOException, ZarrException {
    return create(Paths.get(path), groupMetadata);
  }

  public static Group create(Path path) throws IOException, ZarrException {
    return create(new FilesystemStore(path).resolve());
  }

  public static Group create(String path) throws IOException, ZarrException {
    return create(Paths.get(path));
  }

  @Nullable
  public Node get(String key) throws ZarrException, IOException{
    StoreHandle keyHandle = storeHandle.resolve(key);
    try {
      return Node.open(keyHandle);
    } catch (NoSuchFileException e) {
      return null;
    }
  }

  public Group createGroup(String key, GroupMetadata groupMetadata)
      throws IOException, ZarrException {
    return Group.create(storeHandle.resolve(key), groupMetadata);
  }

  public Group createGroup(String key, Attributes attributes)
      throws IOException, ZarrException {
    return Group.create(storeHandle.resolve(key), new GroupMetadata(attributes));
  }

  public Group createGroup(String key) throws IOException, ZarrException {
    return Group.create(storeHandle.resolve(key), GroupMetadata.defaultValue());
  }

  public Array createArray(String key, ArrayMetadata arrayMetadata)
      throws IOException, ZarrException {
    return Array.create(storeHandle.resolve(key), arrayMetadata);
  }

  public Array createArray(String key,
      Function<ArrayMetadataBuilder, ArrayMetadataBuilder> arrayMetadataBuilderMapper)
      throws IOException, ZarrException {
    return Array.create(storeHandle.resolve(key), arrayMetadataBuilderMapper, false);
  }

  private Group writeMetadata(GroupMetadata newGroupMetadata) throws IOException {
    ObjectWriter objectWriter = makeObjectWriter();
    ByteBuffer metadataBytes = ByteBuffer.wrap(objectWriter.writeValueAsBytes(newGroupMetadata));
    storeHandle.resolve(ZARR_JSON)
        .set(metadataBytes);
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
    return String.format("<v3.Group {%s}>", storeHandle);
  }

  @Override
  public GroupMetadata metadata() {
    return metadata;
  }
}
