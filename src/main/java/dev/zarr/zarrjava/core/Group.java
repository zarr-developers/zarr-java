package dev.zarr.zarrjava.core;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.StoreHandle;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.stream.Stream;

public abstract class Group extends Node {

  protected Group(@Nonnull StoreHandle storeHandle) {
    super(storeHandle);
  }

  @Nullable
  public abstract Node get(String key) throws ZarrException;

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
}
