package dev.zarr.zarrjava.experimental.ome.v0_4;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.experimental.ome.MultiscaleImage;
import dev.zarr.zarrjava.experimental.ome.OmeV2Group;
import dev.zarr.zarrjava.experimental.ome.metadata.WellMetadata;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v2.Group;
import dev.zarr.zarrjava.v2.GroupMetadata;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * OME-Zarr v0.4 HCS well backed by a Zarr v2 group.
 */
public final class Well extends OmeV2Group implements dev.zarr.zarrjava.experimental.ome.Well {

    private WellMetadata wellMetadata;

    private Well(
            @Nonnull StoreHandle storeHandle,
            @Nonnull GroupMetadata groupMetadata,
            @Nonnull WellMetadata wellMetadata
    ) {
        super(storeHandle, groupMetadata);
        this.wellMetadata = wellMetadata;
    }

    /**
     * Opens an existing OME-Zarr v0.4 well at the given store handle.
     */
    public static Well openWell(@Nonnull StoreHandle storeHandle) throws IOException, ZarrException {
        Group group = Group.open(storeHandle);
        WellMetadata wellMetadata = readAttribute(
                group.metadata.attributes, storeHandle, "well", WellMetadata.class);
        return new Well(storeHandle, group.metadata, wellMetadata);
    }

    /**
     * Creates a new OME-Zarr v0.4 well at the given store handle.
     */
    public static Well createWell(
            @Nonnull StoreHandle storeHandle,
            @Nonnull WellMetadata wellMetadata
    ) throws IOException, ZarrException {
        Group group = Group.create(storeHandle, buildAttributes("well", wellMetadata));
        return new Well(storeHandle, group.metadata, wellMetadata);
    }

    @Override
    public WellMetadata getWellMetadata() throws ZarrException {
        return wellMetadata;
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
