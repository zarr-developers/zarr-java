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
        // Try version >= 0.5: zarr.json with "ome" -> "well"
        StoreHandle zarrJson = storeHandle.resolve(Node.ZARR_JSON);
        if (zarrJson.exists()) {
            com.fasterxml.jackson.databind.ObjectMapper mapper = OmeObjectMappers.makeV3Mapper();
            byte[] bytes = Utils.toArray(zarrJson.readNonNull());
            com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(bytes);
            com.fasterxml.jackson.databind.JsonNode attrs = root.get("attributes");
            if (attrs != null && attrs.has("ome") && attrs.get("ome").has("well")) {
                com.fasterxml.jackson.databind.JsonNode omeNode = attrs.get("ome");
                String version = omeNode.has("version") ? omeNode.get("version").asText() : "";
                if (version.startsWith("0.6")) {
                    return dev.zarr.zarrjava.experimental.ome.v0_6.Well.openWell(storeHandle);
                }
                return dev.zarr.zarrjava.experimental.ome.v0_5.Well.openWell(storeHandle);
            }
        }

        // Try v0.4: .zattrs with "well"
        StoreHandle zattrs = storeHandle.resolve(Node.ZATTRS);
        if (zattrs.exists()) {
            com.fasterxml.jackson.databind.ObjectMapper mapper = OmeObjectMappers.makeV2Mapper();
            byte[] bytes = Utils.toArray(zattrs.readNonNull());
            com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(bytes);
            if (root.has("well")) {
                return dev.zarr.zarrjava.experimental.ome.v0_4.Well.openWell(storeHandle);
            }
        }

        throw new ZarrException("No OME-Zarr well metadata found at " + storeHandle);
    }
}
