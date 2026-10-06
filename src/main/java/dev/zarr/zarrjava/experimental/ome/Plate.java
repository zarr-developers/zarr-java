package dev.zarr.zarrjava.experimental.ome;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Node;
import dev.zarr.zarrjava.experimental.ome.metadata.PlateMetadata;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.Utils;

import java.io.IOException;

/**
 * Unified interface for reading OME-Zarr HCS plates across Zarr format versions.
 */
public interface Plate {

    /**
     * Returns the plate metadata.
     */
    PlateMetadata getPlateMetadata() throws ZarrException;

    /**
     * Opens the well at the given row/column path (e.g. {@code "A/1"}).
     */
    Well openWell(String rowColPath) throws IOException, ZarrException;

    /**
     * Returns the store handle for this plate node.
     */
    StoreHandle getStoreHandle();

    /**
     * Opens an OME-Zarr plate at the given store handle, auto-detecting the Zarr version.
     */
    static Plate open(StoreHandle storeHandle) throws IOException, ZarrException {
        // Zarr v3 (OME-Zarr 0.5 and 0.6): a zarr.json holding an "ome" -> "plate" attribute. The group
        // is read once here and handed to the version class, which does not read it again.
        dev.zarr.zarrjava.v3.Group v3Group = OmeNodes.openV3GroupOrNull(storeHandle);
        if (v3Group != null && OmeNodes.omeHas(v3Group.metadata.attributes, "plate")) {
            return fromGroup(v3Group);
        }

        // Zarr v2 (OME-Zarr 0.4): a .zattrs holding a "plate" key.
        dev.zarr.zarrjava.v2.Group v2Group = OmeNodes.openV2GroupOrNull(storeHandle);
        if (v2Group != null && v2Group.metadata.attributes != null
                && v2Group.metadata.attributes.containsKey("plate")) {
            return dev.zarr.zarrjava.experimental.ome.v0_4.Plate.fromGroup(v2Group);
        }

        throw new ZarrException("No OME-Zarr plate metadata found at " + storeHandle);
    }

    /**
     * Builds a plate from a Zarr v3 group that is already open, picking the OME-Zarr version from its
     * attributes. No store request is made, so a group that came from {@code Group.get()} is served
     * from the consolidated metadata of its ancestor.
     */
    static Plate fromGroup(dev.zarr.zarrjava.v3.Group group) throws IOException, ZarrException {
        String version = OmeNodes.omeVersion(group.metadata.attributes);
        if (version != null && version.startsWith("0.6")) {
            return dev.zarr.zarrjava.experimental.ome.v0_6.Plate.fromGroup(group);
        }
        return dev.zarr.zarrjava.experimental.ome.v0_5.Plate.fromGroup(group);
    }
}
