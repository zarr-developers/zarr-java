package dev.zarr.zarrjava.core;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.stream.Stream;

public abstract class Group extends AbstractNode {

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
    public abstract Node get(String key) throws ZarrException;

    public Stream<Node> list() {
        return storeHandle.list()
            .map(key -> {
                try {
                    return get(key);
                } catch (ZarrException e) {
                    throw new RuntimeException(e);
                }
            })
            .filter(Objects::nonNull);
    }

    public Node[] listAsArray() {
        try (Stream<Node> nodeStream = list()) {
            return nodeStream.toArray(Node[]::new);
        }
    }

    public abstract GroupMetadata metadata();
}
