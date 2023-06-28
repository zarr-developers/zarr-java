package com.scalableminds.zarrjava.v3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalableminds.zarrjava.ZarrException;
import com.scalableminds.zarrjava.store.StoreHandle;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
        .readValue(metadataBytes.array(), GroupMetadata.class));
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
    byte[] metadataBytearray = metadataBytes.array();
    try {
      String nodeType = objectMapper.readTree(metadataBytes.array())
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

  public Node[] list() {
    return Arrays.stream(storeHandle.list())
        .map(key -> {
          try {
            return get(key);
          } catch (ZarrException e) {
            throw new RuntimeException(e);
          }
        })
        .filter(Objects::nonNull)
        .toArray(Node[]::new);
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
