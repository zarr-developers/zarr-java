package dev.zarr.zarrjava.core;

import dev.zarr.zarrjava.store.StoreHandle;
import javax.annotation.Nonnull;

public abstract class Node {

  @Nonnull
  public final StoreHandle storeHandle;

  protected Node(@Nonnull StoreHandle storeHandle) {
    this.storeHandle = storeHandle;
  }

}
