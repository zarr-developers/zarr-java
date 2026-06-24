package dev.zarr.zarrjava.experimental.ome.v0_4;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.experimental.ome.OmeV2Group;
import dev.zarr.zarrjava.experimental.ome.metadata.PlateMetadata;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v2.Group;
import dev.zarr.zarrjava.v2.GroupMetadata;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * OME-Zarr v0.4 HCS plate backed by a Zarr v2 group.
 */
public final class Plate extends OmeV2Group implements dev.zarr.zarrjava.experimental.ome.Plate {

    private PlateMetadata plateMetadata;

    private Plate(
            @Nonnull StoreHandle storeHandle,
            @Nonnull GroupMetadata groupMetadata,
            @Nonnull PlateMetadata plateMetadata
    ) {
        super(storeHandle, groupMetadata);
        this.plateMetadata = plateMetadata;
    }

    /**
     * Opens an existing OME-Zarr v0.4 plate at the given store handle.
     */
    public static Plate openPlate(@Nonnull StoreHandle storeHandle) throws IOException, ZarrException {
        Group group = Group.open(storeHandle);
        PlateMetadata plateMetadata = readAttribute(
                group.metadata.attributes, storeHandle, "plate", PlateMetadata.class);
        return new Plate(storeHandle, group.metadata, plateMetadata);
    }

    /**
     * Creates a new OME-Zarr v0.4 plate at the given store handle.
     */
    public static Plate createPlate(
            @Nonnull StoreHandle storeHandle,
            @Nonnull PlateMetadata plateMetadata
    ) throws IOException, ZarrException {
        Group group = Group.create(storeHandle, buildAttributes("plate", plateMetadata));
        return new Plate(storeHandle, group.metadata, plateMetadata);
    }

    @Override
    public PlateMetadata getPlateMetadata() throws ZarrException {
        return plateMetadata;
    }

    @Override
    public dev.zarr.zarrjava.experimental.ome.Well openWell(String rowColPath) throws IOException, ZarrException {
        return Well.openWell(storeHandle.resolve(rowColPath));
    }

    @Override
    public StoreHandle getStoreHandle() {
        return this.storeHandle;
    }
}
