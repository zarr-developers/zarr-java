package dev.zarr.zarrjava.core;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.Utils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class Group extends AbstractNode {

    /**
     * Keys that hold metadata of the group itself and never point at a child node.
     */
    private static final Set<String> METADATA_KEYS = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(ZARR_JSON, ZARRAY, ZATTRS, ZGROUP)));

    protected Group(@Nonnull StoreHandle storeHandle) {
        super(storeHandle);
    }


    /**
     * Opens an existing Zarr group at a specified storage location. Automatically detects the Zarr version.
     *
     * @param storeHandle the storage location of the Zarr group
     * @throws IOException   throws IOException if the metadata cannot be read
     * @throws ZarrException throws ZarrException if the Zarr group cannot be opened
     */
    public static Group open(StoreHandle storeHandle) throws IOException, ZarrException {
        boolean isV3 = storeHandle.resolve(ZARR_JSON).exists();
        boolean isV2 = storeHandle.resolve(ZGROUP).exists();
        if (isV3 && isV2) {
            throw new ZarrException("Both Zarr v2 and v3 groups found at " + storeHandle);
        } else if (isV3) {
            return dev.zarr.zarrjava.v3.Group.open(storeHandle);
        } else if (isV2) {
            return dev.zarr.zarrjava.v2.Group.open(storeHandle);
        } else {
            throw new ZarrException("No Zarr group found at " + storeHandle);
        }
    }


    /**
     * Opens an existing Zarr group at a specified storage location. Automatically detects the Zarr version.
     *
     * @param path the storage location of the Zarr group
     * @throws IOException   throws IOException if the metadata cannot be read
     * @throws ZarrException throws ZarrException if the Zarr group cannot be opened
     */
    public static Group open(Path path) throws IOException, ZarrException {
        return open(new StoreHandle(new FilesystemStore(path)));
    }

    /**
     * Opens an existing Zarr group at a specified storage location. Automatically detects the Zarr version.
     *
     * @param path the storage location of the Zarr group
     * @throws IOException   throws IOException if the metadata cannot be read
     * @throws ZarrException throws ZarrException if the Zarr group cannot be opened
     */
    public static Group open(String path) throws IOException, ZarrException {
        return open(Paths.get(path));
    }

    @Nullable
    public abstract Node get(String[] key) throws ZarrException, IOException;

    @Nullable
    public Node get(String key) throws ZarrException, IOException {
        return get(new String[]{key});
    }

    /**
     * Lists the immediate children (arrays and subgroups) of this group.
     * <p>
     * This costs a single listing request on the underlying store, plus one metadata read per
     * child. Keys that do not hold a Zarr node are skipped.
     *
     * @return a stream of the direct children of this group
     * @throws UnsupportedOperationException if the underlying store does not support listing
     */
    public Stream<Node> members() {
        return childKeys(new String[0]).parallelStream()
                .map(this::openChild)
                .filter(Objects::nonNull)
                .collect(Collectors.toList())
                .stream();
    }

    public Node[] membersAsArray() {
        try (Stream<Node> nodeStream = members()) {
            return nodeStream.toArray(Node[]::new);
        }
    }

    /**
     * Recursively lists all descendants (arrays and groups) of this group, at any depth.
     * <p>
     * The group hierarchy is walked one level at a time, so only group keys are listed and chunk
     * keys are never enumerated. Descending into an array is not necessary and does not happen.
     *
     * @return a stream of all descendants of this group, excluding the group itself
     * @throws UnsupportedOperationException if the underlying store does not support listing
     */
    public Stream<Node> list() {
        return listDescendants(new String[0]);
    }

    public Node[] listAsArray() {
        try (Stream<Node> nodeStream = list()) {
            return nodeStream.toArray(Node[]::new);
        }
    }

    private Stream<Node> listDescendants(String[] prefix) {
        List<Map.Entry<String[], Node>> children = childKeys(prefix).parallelStream()
                .map(key -> new AbstractMap.SimpleEntry<String[], Node>(key, openChild(key)))
                .collect(Collectors.toList());

        return children.stream().flatMap(child -> {
            Node node = child.getValue();
            if (node == null) {
                // Not a node itself, but it may still contain nodes further down.
                return listDescendants(child.getKey());
            }
            if (node instanceof Group) {
                return Stream.concat(Stream.of(node), listDescendants(child.getKey()));
            }
            return Stream.of(node);
        });
    }

    /**
     * Lists the keys directly below {@code prefix} that may hold a child node, relative to this
     * group.
     */
    private List<String[]> childKeys(String[] prefix) {
        try (Stream<String> children = storeHandle.resolve(prefix).listChildren()) {
            return children
                    .filter(name -> !METADATA_KEYS.contains(name))
                    .map(name -> Utils.concatArrays(prefix, new String[]{name}))
                    .collect(Collectors.toList());
        }
    }

    /**
     * Opens the node at {@code key}, or returns null if there is no node there.
     */
    @Nullable
    private Node openDescendant(String[] key) {
        try {
            return get(key);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to read node metadata for key '" + String.join("/", key) + "': " + e.getMessage(), e);
        } catch (ZarrException e) {
            throw new RuntimeException(
                    "Failed to parse node metadata for key '" + String.join("/", key) + "': " + e.getMessage(), e);
        }
    }

    public abstract GroupMetadata metadata();
}
