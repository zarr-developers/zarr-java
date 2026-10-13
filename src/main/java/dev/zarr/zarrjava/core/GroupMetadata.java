package dev.zarr.zarrjava.core;

import dev.zarr.zarrjava.ZarrException;

import javax.annotation.Nonnull;

public abstract class GroupMetadata {

    public @Nonnull
    abstract Attributes attributes() throws ZarrException;

}
