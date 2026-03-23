package dev.zarr.zarrjava.experimental.ome;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Node;
import dev.zarr.zarrjava.experimental.ome.metadata.MultiscalesEntry;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Unified interface for reading OME-Zarr multiscale images across Zarr format versions.
 */
public interface MultiscaleImage {

    /**
     * Returns the store handle for this multiscale image node.
     */
    StoreHandle getStoreHandle();

    /**
     * Returns a {@link MultiscalesEntry} view of multiscale {@code i}, normalized to the shared
     * metadata type. All axis and dataset information is accessible from the returned entry.
     */
    MultiscalesEntry getMultiscaleNode(int i) throws ZarrException;

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
        MultiscalesEntry entry = getMultiscaleNode(0);
        List<String> names = new ArrayList<>();
        for (dev.zarr.zarrjava.experimental.ome.metadata.Axis axis : entry.axes) {
            names.add(axis.name);
        }
        return names;
    }

    /**
     * Returns all label names from the {@code labels/} sub-group, or an empty list if none exist.
     */
    default List<String> getLabels() throws IOException, ZarrException {
        StoreHandle labelsHandle = getStoreHandle().resolve("labels");

        // Try v0.5: labels/zarr.json with {"attributes": {"labels": [...]}}
        StoreHandle zarrJson = labelsHandle.resolve(Node.ZARR_JSON);
        if (zarrJson.exists()) {
            com.fasterxml.jackson.databind.ObjectMapper mapper = dev.zarr.zarrjava.v3.Node.makeObjectMapper();
            byte[] bytes = Utils.toArray(zarrJson.readNonNull());
            com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(bytes);
            com.fasterxml.jackson.databind.JsonNode attrs = root.get("attributes");
            if (attrs != null && attrs.has("labels")) {
                com.fasterxml.jackson.databind.JsonNode labelsNode = attrs.get("labels");
                List<String> result = new ArrayList<>();
                for (com.fasterxml.jackson.databind.JsonNode item : labelsNode) {
                    result.add(item.asText());
                }
                return result;
            }
        }

        // Try v0.4: labels/.zattrs with {"labels": [...]}
        StoreHandle zattrs = labelsHandle.resolve(Node.ZATTRS);
        if (zattrs.exists()) {
            com.fasterxml.jackson.databind.ObjectMapper mapper = dev.zarr.zarrjava.v2.Node.makeObjectMapper();
            byte[] bytes = Utils.toArray(zattrs.readNonNull());
            com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(bytes);
            if (root.has("labels")) {
                com.fasterxml.jackson.databind.JsonNode labelsNode = root.get("labels");
                List<String> result = new ArrayList<>();
                for (com.fasterxml.jackson.databind.JsonNode item : labelsNode) {
                    result.add(item.asText());
                }
                return result;
            }
        }

        return Collections.emptyList();
    }

    /**
     * Opens the named label image from the {@code labels/} sub-group.
     */
    default MultiscaleImage openLabel(String name) throws IOException, ZarrException {
        return MultiscaleImage.open(getStoreHandle().resolve("labels").resolve(name));
    }

    /**
     * Opens an OME-Zarr multiscale image at the given store handle, auto-detecting the Zarr version.
     *
     * <p>Tries v0.6 (zarr.json with version "0.6"), then v0.5 (zarr.json with "ome" key), then v0.4 (.zattrs with "multiscales" key).
     */
    static MultiscaleImage open(StoreHandle storeHandle) throws IOException, ZarrException {
        // Try version >= 0.5: zarr.json with "ome" key
        StoreHandle zarrJson = storeHandle.resolve(Node.ZARR_JSON);
        if (zarrJson.exists()) {
            com.fasterxml.jackson.databind.ObjectMapper mapper = OmeObjectMappers.makeV3Mapper();
            byte[] bytes = Utils.toArray(zarrJson.readNonNull());
            com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(bytes);
            com.fasterxml.jackson.databind.JsonNode attrs = root.get("attributes");
            if (attrs != null && attrs.has("ome")) {
                com.fasterxml.jackson.databind.JsonNode omeNode = attrs.get("ome");
                String version = omeNode.has("version") ? omeNode.get("version").asText() : "";
                if (version.startsWith("0.6")) {
                    return dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage.openMultiscaleImage(storeHandle);
                }
                return dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage.openMultiscaleImage(storeHandle);
            }
        }

        // Try v0.4: .zattrs with "multiscales" key
        StoreHandle zattrs = storeHandle.resolve(Node.ZATTRS);
        if (zattrs.exists()) {
            com.fasterxml.jackson.databind.ObjectMapper mapper = OmeObjectMappers.makeV2Mapper();
            byte[] bytes = Utils.toArray(zattrs.readNonNull());
            com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(bytes);
            if (root.has("multiscales")) {
                return dev.zarr.zarrjava.experimental.ome.v0_4.MultiscaleImage.openMultiscaleImage(storeHandle);
            }
        }

        throw new ZarrException("No OME-Zarr multiscale metadata found at " + storeHandle);
    }
}
