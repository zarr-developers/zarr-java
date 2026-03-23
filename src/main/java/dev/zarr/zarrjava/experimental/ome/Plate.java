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
        // Try version >= 0.5: zarr.json with "ome" -> "plate"
        StoreHandle zarrJson = storeHandle.resolve(Node.ZARR_JSON);
        if (zarrJson.exists()) {
            com.fasterxml.jackson.databind.ObjectMapper mapper = OmeObjectMappers.makeV3Mapper();
            byte[] bytes = Utils.toArray(zarrJson.readNonNull());
            com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(bytes);
            com.fasterxml.jackson.databind.JsonNode attrs = root.get("attributes");
            if (attrs != null && attrs.has("ome") && attrs.get("ome").has("plate")) {
                com.fasterxml.jackson.databind.JsonNode omeNode = attrs.get("ome");
                String version = omeNode.has("version") ? omeNode.get("version").asText() : "";
                if (version.startsWith("0.6")) {
                    return dev.zarr.zarrjava.experimental.ome.v0_6.Plate.openPlate(storeHandle);
                }
                return dev.zarr.zarrjava.experimental.ome.v0_5.Plate.openPlate(storeHandle);
            }
        }

        // Try v0.4: .zattrs with "plate"
        StoreHandle zattrs = storeHandle.resolve(Node.ZATTRS);
        if (zattrs.exists()) {
            com.fasterxml.jackson.databind.ObjectMapper mapper = OmeObjectMappers.makeV2Mapper();
            byte[] bytes = Utils.toArray(zattrs.readNonNull());
            com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(bytes);
            if (root.has("plate")) {
                return dev.zarr.zarrjava.experimental.ome.v0_4.Plate.openPlate(storeHandle);
            }
        }

        throw new ZarrException("No OME-Zarr plate metadata found at " + storeHandle);
    }
}
