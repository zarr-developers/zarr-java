package dev.zarr.zarrjava.core;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

public interface Node {

    String ZARR_JSON = "zarr.json";
    String ZARRAY = ".zarray";
    String ZATTRS = ".zattrs";
    String ZGROUP = ".zgroup";

    /**
     * Opens an existing Zarr array or group at a specified storage location. Automatically detects the Zarr version.
     *
     * @param storeHandle the storage location of the Zarr array
     * @throws IOException   throws IOException if the metadata cannot be read
     * @throws ZarrException throws ZarrException if the Zarr array cannot be opened
     */
    static Node open(StoreHandle storeHandle) throws IOException, ZarrException {
        boolean isV3 = storeHandle.resolve(ZARR_JSON).exists();
        boolean isV2 = storeHandle.resolve(ZARRAY).exists() || storeHandle.resolve(ZGROUP).exists();

        if (isV3 && isV2) {
            throw new ZarrException("Both Zarr v2 and v3 nodes found at " + storeHandle);
        } else if (isV3) {
            return dev.zarr.zarrjava.v3.Node.open(storeHandle);
        } else if (isV2) {
            return dev.zarr.zarrjava.v2.Node.open(storeHandle);
        } else {
            throw new NoSuchFileException("No Zarr node found at " + storeHandle);
        }
    }

    /**
     * Opens an existing Zarr array or group at a specified storage location. Automatically detects the Zarr version.
     *
     * @param path the storage location of the Zarr array
     * @throws IOException   throws IOException if the metadata cannot be read
     * @throws ZarrException throws ZarrException if the Zarr array cannot be opened
     */
    static Node open(Path path) throws IOException, ZarrException {
        return open(new StoreHandle(new FilesystemStore(path)));
    }

    /**
     * Opens an existing Zarr array or group at a specified storage location. Automatically detects the Zarr version.
     *
     * @param path the storage location of the Zarr array
     * @throws IOException   throws IOException if the metadata cannot be read
     * @throws ZarrException throws ZarrException if the Zarr array cannot be opened
     */
    static Node open(String path) throws IOException, ZarrException {
        return open(Paths.get(path));
    }
}

