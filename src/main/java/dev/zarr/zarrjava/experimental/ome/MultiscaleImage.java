package dev.zarr.zarrjava.experimental.ome;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Node;
import dev.zarr.zarrjava.experimental.ome.metadata.MultiscalesEntry;
import dev.zarr.zarrjava.experimental.ome.metadata.OmeroMetadata;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.Utils;

import javax.annotation.Nullable;
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
     * Returns the OmeroMetadata if present, or null if not.
     */
    @Nullable
    OmeroMetadata getOmeroMetadata();

    /**
     * Returns the bioformats2raw.layout property if present, or null if not
     */
    @Nullable
    Integer getBioformats2rawLayout();

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
        dev.zarr.zarrjava.v3.Group v3Group = asV3Group();
        if (v3Group != null) {
            // Walk down through the group, so that a consolidated ancestor answers without a request.
            dev.zarr.zarrjava.core.Node labelsNode = v3Group.get(new String[]{"labels"});
            if (!(labelsNode instanceof dev.zarr.zarrjava.v3.Group)) {
                return Collections.emptyList();
            }
            dev.zarr.zarrjava.core.Attributes labelsAttributes =
                    ((dev.zarr.zarrjava.v3.Group) labelsNode).metadata.attributes;
            if (labelsAttributes == null || !labelsAttributes.containsKey("labels")) {
                return Collections.emptyList();
            }
            List<String> result = new ArrayList<>();
            for (Object item : labelsAttributes.getList("labels")) {
                result.add(String.valueOf(item));
            }
            return result;
        }

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
        dev.zarr.zarrjava.v3.Group v3Group = asV3Group();
        if (v3Group != null) {
            return fromGroup(OmeNodes.childGroup(v3Group, "labels/" + name));
        }
        return MultiscaleImage.open(getStoreHandle().resolve("labels").resolve(name));
    }

    /**
     * Opens an OME-Zarr multiscale image at the given store handle, auto-detecting the Zarr version.
     *
     * <p>Tries v0.6 (zarr.json with version "0.6"), then v0.5 (zarr.json with "ome" key), then v0.4 (.zattrs with "multiscales" key).
     */
    static MultiscaleImage open(StoreHandle storeHandle) throws IOException, ZarrException {
        // Zarr v3 (OME-Zarr 0.5 and 0.6): a zarr.json holding an "ome" attribute. The group is read once
        // here and handed to the version class, which does not read it again.
        dev.zarr.zarrjava.v3.Group v3Group = OmeNodes.openV3GroupOrNull(storeHandle);
        if (v3Group != null && OmeNodes.omeAttributes(v3Group.metadata.attributes) != null) {
            return fromGroup(v3Group);
        }

        // Zarr v2 (OME-Zarr 0.4): a .zattrs holding a "multiscales" key.
        dev.zarr.zarrjava.v2.Group v2Group = OmeNodes.openV2GroupOrNull(storeHandle);
        if (v2Group != null && v2Group.metadata.attributes != null
                && v2Group.metadata.attributes.containsKey("multiscales")) {
            return dev.zarr.zarrjava.experimental.ome.v0_4.MultiscaleImage.fromGroup(v2Group);
        }

        throw new ZarrException("No OME-Zarr multiscale metadata found at " + storeHandle);
    }

    /**
     * Builds a multiscale image from a Zarr v3 group that is already open, picking the OME-Zarr version
     * from its attributes. No store request is made, so a group that came from {@code Group.get()} is
     * served from the consolidated metadata of its ancestor.
     */
    static MultiscaleImage fromGroup(dev.zarr.zarrjava.v3.Group group) throws IOException, ZarrException {
        String version = OmeNodes.omeVersion(group.metadata.attributes);
        if (version != null && version.startsWith("0.6")) {
            return dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage.fromGroup(group);
        }
        return dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage.fromGroup(group);
    }

    /**
     * The Zarr v3 group backing this image, or null for OME-Zarr 0.4, which is backed by a Zarr v2
     * group. Used by {@link #getLabels()} and {@link #openLabel(String)} to walk down through the group
     * - and therefore through its consolidated metadata - rather than by re-reading the store.
     */
    @Nullable
    default dev.zarr.zarrjava.v3.Group asV3Group() {
        return null;
    }
}
