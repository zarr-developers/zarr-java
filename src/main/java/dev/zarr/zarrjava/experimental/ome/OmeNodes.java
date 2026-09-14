package dev.zarr.zarrjava.experimental.ome;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.Group;
import dev.zarr.zarrjava.v3.Node;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.NoSuchFileException;

/**
 * Helpers shared by the OME-Zarr nodes for opening a node once and for walking down a hierarchy
 * through the group that is already open.
 * <p>
 * Walking down through {@link Group#get} instead of resolving a fresh {@link StoreHandle} is what
 * lets an OME-Zarr hierarchy benefit from consolidated metadata: a group that has a cache answers
 * for all of its descendants without touching the store. Resolving a handle and opening it again
 * always costs a request per node, because a handle is only a path and carries no metadata.
 */
public final class OmeNodes {

    private OmeNodes() {
    }

    /**
     * Opens the Zarr v3 group at {@code storeHandle}, or returns null if the node is not a Zarr v3
     * group: either there is no {@code zarr.json} there, or it describes an array. Reads
     * {@code zarr.json} once.
     */
    @Nullable
    public static Group openV3GroupOrNull(@Nonnull StoreHandle storeHandle)
            throws IOException, ZarrException {
        try {
            Node node = Node.open(storeHandle);
            return node instanceof Group ? (Group) node : null;
        } catch (NoSuchFileException e) {
            return null;
        }
    }

    /**
     * Opens the Zarr v2 group at {@code storeHandle}, or returns null if there is no {@code .zgroup}
     * there, meaning the node is not a Zarr v2 group.
     */
    @Nullable
    public static dev.zarr.zarrjava.v2.Group openV2GroupOrNull(@Nonnull StoreHandle storeHandle)
            throws IOException {
        try {
            return dev.zarr.zarrjava.v2.Group.open(storeHandle);
        } catch (NoSuchFileException e) {
            return null;
        }
    }

    /**
     * Returns the {@code version} of the {@code ome} attribute, or null if the attributes hold no
     * usable {@code ome} entry. Used to pick the OME-Zarr version of a node whose metadata has
     * already been read.
     */
    @Nullable
    public static String omeVersion(@Nullable Attributes attributes) {
        Attributes ome = omeAttributes(attributes);
        if (ome == null) {
            return null;
        }
        Object version = ome.get("version");
        return version == null ? null : version.toString();
    }

    /**
     * Whether the {@code ome} attribute holds {@code key}, for example {@code "plate"},
     * {@code "well"} or {@code "multiscales"}.
     */
    public static boolean omeHas(@Nullable Attributes attributes, @Nonnull String key) {
        Attributes ome = omeAttributes(attributes);
        return ome != null && ome.get(key) != null;
    }

    /**
     * The {@code ome} attribute as {@link Attributes}, or null if it is absent or not a mapping.
     */
    @Nullable
    public static Attributes omeAttributes(@Nullable Attributes attributes) {
        if (attributes == null || !attributes.containsKey("ome")) {
            return null;
        }
        try {
            return attributes.getAttributes("ome");
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Returns the child group at {@code path} below {@code parent}, using the consolidated metadata of
     * {@code parent} if it has any.
     *
     * @param path the path of the child relative to {@code parent}, {@code "/"}-separated
     * @throws ZarrException if there is no node at {@code path}, or if it is not a group
     */
    @Nonnull
    public static Group childGroup(@Nonnull Group parent, @Nonnull String path)
            throws IOException, ZarrException {
        Node child = child(parent, path);
        if (!(child instanceof Group)) {
            throw new ZarrException(
                    "'" + path + "' below " + parent.storeHandle + " is not a group.");
        }
        return (Group) child;
    }

    /**
     * Returns the child array at {@code path} below {@code parent}, using the consolidated metadata of
     * {@code parent} if it has any.
     *
     * @param path the path of the child relative to {@code parent}, {@code "/"}-separated
     * @throws ZarrException if there is no node at {@code path}, or if it is not an array
     */
    @Nonnull
    public static Array childArray(@Nonnull Group parent, @Nonnull String path)
            throws IOException, ZarrException {
        Node child = child(parent, path);
        if (!(child instanceof Array)) {
            throw new ZarrException(
                    "'" + path + "' below " + parent.storeHandle + " is not an array.");
        }
        return (Array) child;
    }

    @Nonnull
    private static Node child(@Nonnull Group parent, @Nonnull String path)
            throws IOException, ZarrException {
        Node child = parent.get(path.split("/"));
        if (child == null) {
            throw new ZarrException("No node at '" + path + "' below " + parent.storeHandle
                    + ". If the group has consolidated metadata, that cache is a snapshot and may be"
                    + " stale; consolidate it again, or open the group with UseConsolidated.IGNORE.");
        }
        return child;
    }

    /**
     * Returns the names of the direct children of {@code group} without listing the store if
     * {@code group} has consolidated metadata, and by listing the store otherwise.
     */
    @Nonnull
    public static java.util.List<String> childNames(@Nonnull Group group) {
        java.util.List<String> names = new java.util.ArrayList<>();
        if (group.metadata.consolidatedMetadata != null) {
            for (String key : group.metadata.consolidatedMetadata.metadata.keySet()) {
                if (key.indexOf('/') < 0) {
                    names.add(key);
                }
            }
            return names;
        }
        try (java.util.stream.Stream<String> children = group.storeHandle.listChildren()) {
            children.forEach(names::add);
        }
        return names;
    }
}
