package dev.zarr.zarrjava.core;

import javax.annotation.Nonnull;

import dev.zarr.zarrjava.ZarrException;

public abstract class GroupMetadata {

    public @Nonnull abstract Attributes attributes() throws ZarrException;

}
