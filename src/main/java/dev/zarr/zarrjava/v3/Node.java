package dev.zarr.zarrjava.v3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.Utils;
import dev.zarr.zarrjava.v3.codec.CodecRegistry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;


public interface Node extends dev.zarr.zarrjava.core.Node{

  static ObjectMapper makeObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(new Jdk8Module());
    objectMapper.registerSubtypes(CodecRegistry.getNamedTypes());
    return objectMapper;
  }

  /**
   * Opens an existing Zarr array or group at a specified storage location.
   *
   * @param storeHandle the storage location of the Zarr array or group
   * @throws IOException   throws IOException if the metadata cannot be read
   * @throws ZarrException throws ZarrException if the Zarr array or group cannot be opened
   */
  static Node open(StoreHandle storeHandle) throws IOException, ZarrException {
    ObjectMapper objectMapper = makeObjectMapper();
    ByteBuffer metadataBytes = storeHandle.resolve(ZARR_JSON).readNonNull();
    byte[] metadataBytearray = Utils.toArray(metadataBytes);
    String nodeType = objectMapper.readTree(metadataBytearray)
        .get("node_type")
        .asText();
    switch (nodeType) {
      case ArrayMetadata.NODE_TYPE:
        return new Array(storeHandle,
            objectMapper.readValue(metadataBytearray, ArrayMetadata.class));
      case GroupMetadata.NODE_TYPE:
        return new Group(storeHandle,
            objectMapper.readValue(metadataBytearray, GroupMetadata.class));
      default:
        throw new ZarrException("Unsupported node_type '" + nodeType + "' at " + storeHandle);
    }
  }

  static Node open(Path path) throws IOException, ZarrException {
    return open(new StoreHandle(new FilesystemStore(path)));
  }

  static Node open(String path) throws IOException, ZarrException {
    return open(Paths.get(path));
  }
}
