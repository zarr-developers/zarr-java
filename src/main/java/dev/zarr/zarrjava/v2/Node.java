package dev.zarr.zarrjava.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v2.codec.CodecRegistry;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

public interface Node extends dev.zarr.zarrjava.core.Node {

  static ObjectMapper makeObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(new Jdk8Module());
    objectMapper.registerSubtypes(CodecRegistry.getNamedTypes());
    return objectMapper;
  }

  /**
   * Opens an existing Zarr array or group at a specified storage location.
   *
   * @param storeHandle the storage location of the Zarr array
   * @throws IOException   throws IOException if the metadata cannot be read
   * @throws ZarrException throws ZarrException if the Zarr array or group cannot be opened
   */
  static Node open(StoreHandle storeHandle) throws IOException, ZarrException {
    boolean isGroup = storeHandle.resolve(ZGROUP).exists();
    boolean isArray = storeHandle.resolve(ZARRAY).exists();

    if (isGroup && isArray) {
      throw new ZarrException("Store handle '" + storeHandle + "' contains both a " + ZGROUP + " and a " + ZARRAY + " file.");
    } else if (isGroup) {
      return Group.open(storeHandle);
    } else if (isArray) {
      try {
        return Array.open(storeHandle);
      } catch (IOException e) {
        throw new ZarrException("Failed to read array metadata for store handle '" + storeHandle + "'.", e);
      }
    }
    throw new NoSuchFileException("Store handle '" + storeHandle + "' does not contain a " + ZGROUP + " or a " + ZARRAY + " file.");
  }

  static Node open(Path path) throws IOException, ZarrException {
    return open(new StoreHandle(new FilesystemStore(path)));
  }

  static Node open(String path) throws IOException, ZarrException {
    return open(Paths.get(path));
  }
}
