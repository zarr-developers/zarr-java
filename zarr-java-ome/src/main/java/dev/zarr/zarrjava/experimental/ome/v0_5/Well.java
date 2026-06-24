package dev.zarr.zarrjava.experimental.ome.v0_5;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.experimental.ome.MultiscaleImage;
import dev.zarr.zarrjava.experimental.ome.OmeV3Group;
import dev.zarr.zarrjava.experimental.ome.metadata.OmeMetadata;
import dev.zarr.zarrjava.experimental.ome.metadata.WellMetadata;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Group;
import dev.zarr.zarrjava.v3.GroupMetadata;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * OME-Zarr v0.5 HCS well backed by a Zarr v3 group.
 */
public final class Well extends OmeV3Group implements dev.zarr.zarrjava.experimental.ome.Well {

    private OmeMetadata omeMetadata;

    private Well(
            @Nonnull StoreHandle storeHandle,
            @Nonnull GroupMetadata groupMetadata,
            @Nonnull OmeMetadata omeMetadata
    ) throws IOException {
        super(storeHandle, groupMetadata);
        this.omeMetadata = omeMetadata;
    }

    /**
     * Opens an existing OME-Zarr v0.5 well at the given store handle.
     */
    public static Well openWell(@Nonnull StoreHandle storeHandle) throws IOException, ZarrException {
        Group group = Group.open(storeHandle);
        OmeMetadata omeMetadata = readOmeAttribute(
                group.metadata.attributes, storeHandle, OmeMetadata.class);
        if (omeMetadata.well == null) {
            throw new ZarrException("No 'well' found in ome metadata at " + storeHandle);
        }
        return new Well(storeHandle, group.metadata, omeMetadata);
    }

    /**
     * Creates a new OME-Zarr v0.5 well at the given store handle.
     */
    public static Well createWell(
            @Nonnull StoreHandle storeHandle,
            @Nonnull WellMetadata wellMetadata
    ) throws IOException, ZarrException {
        OmeMetadata omeMetadata = new OmeMetadata("0.5", null, null, null, null, wellMetadata);
        Group group = Group.create(storeHandle, omeAttributes(omeMetadata));
        return new Well(storeHandle, group.metadata, omeMetadata);
    }

    @Override
    public WellMetadata getWellMetadata() throws ZarrException {
        return omeMetadata.well;
    }

    @Override
    public MultiscaleImage openImage(String path) throws IOException, ZarrException {
        return MultiscaleImage.open(storeHandle.resolve(path));
    }

    @Override
    public StoreHandle getStoreHandle() {
        return this.storeHandle;
    }
}
