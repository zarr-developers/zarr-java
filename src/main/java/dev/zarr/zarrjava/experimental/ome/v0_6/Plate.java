package dev.zarr.zarrjava.experimental.ome.v0_6;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.experimental.ome.OmeV3Group;
import dev.zarr.zarrjava.experimental.ome.metadata.PlateMetadata;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.OmeMetadata;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Group;
import dev.zarr.zarrjava.v3.GroupMetadata;

import dev.zarr.zarrjava.experimental.ome.OmeNodes;
import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * OME-Zarr v0.6 HCS plate backed by a Zarr v3 group.
 */
public final class Plate extends OmeV3Group implements dev.zarr.zarrjava.experimental.ome.Plate {

    private OmeMetadata omeMetadata;

    private Plate(
            @Nonnull StoreHandle storeHandle,
            @Nonnull GroupMetadata groupMetadata,
            @Nonnull OmeMetadata omeMetadata
    ) throws IOException {
        super(storeHandle, groupMetadata);
        this.omeMetadata = omeMetadata;
    }

    /**
     * Opens an existing OME-Zarr v0.6 plate at the given store handle.
     */
    public static Plate openPlate(@Nonnull StoreHandle storeHandle) throws IOException, ZarrException {
        return fromGroup(Group.open(storeHandle));
    }

    /**
     * Builds an OME-Zarr v0.6 plate from a group that is already open, without reading the store
     * again. Use this when the group came from {@link Group#get}, so that the consolidated metadata
     * of an ancestor is used instead of one request per node.
     */
    public static Plate fromGroup(@Nonnull Group group) throws IOException, ZarrException {
        StoreHandle storeHandle = group.storeHandle;
        OmeMetadata omeMetadata = readOmeAttribute(
                group.metadata.attributes, storeHandle, OmeMetadata.class);
        if (!omeMetadata.version.startsWith("0.6")) {
            throw new ZarrException(
                    "Expected OME-Zarr version '0.6', got '" + omeMetadata.version + "' at " + storeHandle);
        }
        if (omeMetadata.plate == null) {
            throw new ZarrException("No 'plate' found in ome metadata at " + storeHandle);
        }
        return new Plate(storeHandle, group.metadata, omeMetadata);
    }

    /**
     * Creates a new OME-Zarr v0.6 plate at the given store handle.
     */
    public static Plate createPlate(
            @Nonnull StoreHandle storeHandle,
            @Nonnull PlateMetadata plateMetadata
    ) throws IOException, ZarrException {
        OmeMetadata omeMetadata = new OmeMetadata("0.6", null, null, null, null, plateMetadata, null);
        Group group = Group.create(storeHandle, omeAttributes(omeMetadata));
        return new Plate(storeHandle, group.metadata, omeMetadata);
    }

    @Override
    public PlateMetadata getPlateMetadata() throws ZarrException {
        return omeMetadata.plate;
    }

    @Override
    public dev.zarr.zarrjava.experimental.ome.Well openWell(String rowColPath) throws IOException, ZarrException {
        return Well.fromGroup(OmeNodes.childGroup(this, rowColPath));
    }

    @Override
    public StoreHandle getStoreHandle() {
        return this.storeHandle;
    }
}
