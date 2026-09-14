package dev.zarr.zarrjava.experimental.ome;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Node;
import dev.zarr.zarrjava.experimental.ome.metadata.WellMetadata;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.Utils;

import java.io.IOException;

/**
 * Unified interface for reading OME-Zarr HCS wells across Zarr format versions.
 */
public interface Well {

    /**
     * Returns the well metadata.
     */
    WellMetadata getWellMetadata() throws ZarrException;

    /**
     * Opens the image at the given path within this well (e.g. {@code "0"}).
     */
    MultiscaleImage openImage(String path) throws IOException, ZarrException;

    /**
     * Returns the store handle for this well node.
     */
    StoreHandle getStoreHandle();

    /**
     * Opens an OME-Zarr well at the given store handle, auto-detecting the Zarr version.
     */
    static Well open(StoreHandle storeHandle) throws IOException, ZarrException {
        // Zarr v3 (OME-Zarr 0.5 and 0.6): a zarr.json holding an "ome" -> "well" attribute. The group is
        // read once here and handed to the version class, which does not read it again.
        dev.zarr.zarrjava.v3.Group v3Group = OmeNodes.openV3GroupOrNull(storeHandle);
        if (v3Group != null && OmeNodes.omeHas(v3Group.metadata.attributes, "well")) {
            return fromGroup(v3Group);
        }

        // Zarr v2 (OME-Zarr 0.4): a .zattrs holding a "well" key.
        dev.zarr.zarrjava.v2.Group v2Group = OmeNodes.openV2GroupOrNull(storeHandle);
        if (v2Group != null && v2Group.metadata.attributes != null
                && v2Group.metadata.attributes.containsKey("well")) {
            return dev.zarr.zarrjava.experimental.ome.v0_4.Well.fromGroup(v2Group);
        }

        throw new ZarrException("No OME-Zarr well metadata found at " + storeHandle);
    }

    /**
     * Builds a well from a Zarr v3 group that is already open, picking the OME-Zarr version from its
     * attributes. No store request is made, so a group that came from {@code Group.get()} is served
     * from the consolidated metadata of its ancestor.
     */
    static Well fromGroup(dev.zarr.zarrjava.v3.Group group) throws IOException, ZarrException {
        String version = OmeNodes.omeVersion(group.metadata.attributes);
        if (version != null && version.startsWith("0.6")) {
            return dev.zarr.zarrjava.experimental.ome.v0_6.Well.fromGroup(group);
        }
        return dev.zarr.zarrjava.experimental.ome.v0_5.Well.fromGroup(group);
    }
}
