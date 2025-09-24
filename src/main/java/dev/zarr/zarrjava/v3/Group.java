package dev.zarr.zarrjava.v3;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Node;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static dev.zarr.zarrjava.v3.Node.ZARR_JSON;

public class Group extends Node {

  public GroupMetadata metadata;

  Group(@Nonnull StoreHandle storeHandle, @Nonnull GroupMetadata groupMetadata) throws IOException {
    super(storeHandle);
    this.metadata = groupMetadata;
  }

  public static Group open(@Nonnull StoreHandle storeHandle) throws IOException {
    StoreHandle metadataHandle = storeHandle.resolve(ZARR_JSON);
    ByteBuffer metadataBytes = metadataHandle.readNonNull();
    return new Group(storeHandle, Node.makeObjectMapper()
        .readValue(Utils.toArray(metadataBytes), GroupMetadata.class));
  }

  public static Group create(
      @Nonnull StoreHandle storeHandle, @Nonnull GroupMetadata groupMetadata
  ) throws IOException {
    ObjectMapper objectMapper = Node.makeObjectMapper();
    ByteBuffer metadataBytes = ByteBuffer.wrap(objectMapper.writeValueAsBytes(groupMetadata));
    storeHandle.resolve(ZARR_JSON)
        .set(metadataBytes);
    return new Group(storeHandle, groupMetadata);
  }

  public static Group create(
      @Nonnull StoreHandle storeHandle,
      @Nonnull Map<String, Object> attributes
  ) throws IOException, ZarrException {
    return new Group(storeHandle, new GroupMetadata(attributes));
  }

  public static Group create(@Nonnull StoreHandle storeHandle) throws IOException, ZarrException {
    return create(storeHandle, GroupMetadata.defaultValue());
  }

  @Nullable
  public Node get(String key) throws ZarrException {
    StoreHandle keyHandle = storeHandle.resolve(key);
    ObjectMapper objectMapper = Node.makeObjectMapper();
    ByteBuffer metadataBytes = keyHandle.resolve(ZARR_JSON)
        .read();
    if (metadataBytes == null) {
      return null;
    }
    byte[] metadataBytearray = Utils.toArray(metadataBytes);
    try {
      String nodeType = objectMapper.readTree(metadataBytearray)
          .get("node_type")
          .asText();
      switch (nodeType) {
        case ArrayMetadata.NODE_TYPE:
          return new Array(keyHandle,
              objectMapper.readValue(metadataBytearray, ArrayMetadata.class));
        case GroupMetadata.NODE_TYPE:
          return new Group(keyHandle,
              objectMapper.readValue(metadataBytearray, GroupMetadata.class));
        default:
          throw new ZarrException("Unsupported node_type '" + nodeType + "' in " + keyHandle);
      }
    } catch (IOException e) {
      return null;
    }
  }

  public Group createGroup(String key, GroupMetadata groupMetadata)
      throws IOException, ZarrException {
    return Group.create(storeHandle.resolve(key), groupMetadata);
  }

  public Group createGroup(String key, Map<String, Object> attributes)
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

  public Stream<Node> list() {
    return storeHandle.list()
        .map(key -> {
          try {
            return get(key);
          } catch (ZarrException e) {
            throw new RuntimeException(e);
          }
        })
        .filter(Objects::nonNull);
  }

  public Node[] listAsArray() {
    try (Stream<Node> nodeStream = list()) {
      return nodeStream.toArray(Node[]::new);
    }
  }

  private Group writeMetadata(GroupMetadata newGroupMetadata) throws IOException {
    ObjectMapper objectMapper = Node.makeObjectMapper();
    ByteBuffer metadataBytes = ByteBuffer.wrap(objectMapper.writeValueAsBytes(newGroupMetadata));
    storeHandle.resolve(ZARR_JSON)
        .set(metadataBytes);
    return new Group(storeHandle, newGroupMetadata);
  }

  public Group setAttributes(Map<String, Object> newAttributes) throws ZarrException, IOException {
    GroupMetadata newGroupMetadata = new GroupMetadata(newAttributes);
    return writeMetadata(newGroupMetadata);
  }

  public Group updateAttributes(Function<Map<String, Object>, Map<String, Object>> attributeMapper)
      throws ZarrException, IOException {
    return setAttributes(attributeMapper.apply(metadata.attributes));
  }

  @Override
  public String toString() {
    return String.format("<v3.Group {%s}>", storeHandle);
  }
}
