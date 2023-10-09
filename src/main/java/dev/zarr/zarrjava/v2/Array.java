package dev.zarr.zarrjava.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.Utils;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

public class Array {

  static final String ZARRAY = ".zarray";
  public ArrayMetadata metadata;
  public StoreHandle storeHandle;

  Array(StoreHandle storeHandle) throws IOException {
    this.storeHandle = storeHandle;

    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(new Jdk8Module());
    this.metadata = objectMapper.readValue(
        Utils.toArray(storeHandle.resolve(ZARRAY).readNonNull()),
        ArrayMetadata.class
    );
  }

  public static Array open(StoreHandle storeHandle) throws IOException {
    return new Array(storeHandle);
  }

  @Override
  public String toString() {
    return String.format("<v2.Array {%s} (%s) %s>", storeHandle,
        Arrays.stream(metadata.shape)
            .mapToObj(Long::toString)
            .collect(Collectors.joining(", ")),
        metadata.dataType
    );
  }
}
