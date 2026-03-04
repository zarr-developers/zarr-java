package dev.zarr.zarrjava.ome;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Node;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Unified interface for reading OME-Zarr multiscale images across Zarr format versions.
 */
public interface MultiscaleImage {

    /**
     * Returns the multiscale node descriptor at index {@code i}.
     */
    UnifiedMultiscaleNode getMultiscaleNode(int i) throws ZarrException;

    /**
     * Opens the scale level array at index {@code i} within the first multiscale entry.
     */
    dev.zarr.zarrjava.core.Array openScaleLevel(int i) throws IOException, ZarrException;

    /**
     * Returns the number of scale levels in the first multiscale entry.
     */
    int getScaleLevelCount() throws ZarrException;

    /**
     * Returns the axis names of the first multiscale entry.
     */
    default List<String> getAxisNames() throws ZarrException {
        UnifiedMultiscaleNode node = getMultiscaleNode(0);
        List<String> names = new java.util.ArrayList<>();
        for (dev.zarr.zarrjava.ome.metadata.Axis axis : node.axes) {
            names.add(axis.name);
        }
        return names;
    }

    /**
     * Opens an OME-Zarr multiscale image at the given store handle, auto-detecting the Zarr version.
     *
     * <p>Tries v0.5 (zarr.json with "ome" key) first, then v0.4 (.zattrs with "multiscales" key).
     */
    static MultiscaleImage open(StoreHandle storeHandle) throws IOException, ZarrException {
        // Try v0.5: zarr.json with "ome" key
        StoreHandle zarrJson = storeHandle.resolve(Node.ZARR_JSON);
        if (zarrJson.exists()) {
            com.fasterxml.jackson.databind.ObjectMapper mapper = dev.zarr.zarrjava.v3.Node.makeObjectMapper();
            byte[] bytes = Utils.toArray(zarrJson.readNonNull());
            com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(bytes);
            com.fasterxml.jackson.databind.JsonNode attrs = root.get("attributes");
            if (attrs != null && attrs.has("ome")) {
                return dev.zarr.zarrjava.ome.v0_5.MultiscaleImage.openMultiscaleImage(storeHandle);
            }
        }

        // Try v0.4: .zattrs with "multiscales" key
        StoreHandle zattrs = storeHandle.resolve(Node.ZATTRS);
        if (zattrs.exists()) {
            com.fasterxml.jackson.databind.ObjectMapper mapper = dev.zarr.zarrjava.v2.Node.makeObjectMapper();
            byte[] bytes = Utils.toArray(zattrs.readNonNull());
            com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(bytes);
            if (root.has("multiscales")) {
                return dev.zarr.zarrjava.ome.v0_4.MultiscaleImage.openMultiscaleImage(storeHandle);
            }
        }

        throw new ZarrException("No OME-Zarr multiscale metadata found at " + storeHandle);
    }
}
