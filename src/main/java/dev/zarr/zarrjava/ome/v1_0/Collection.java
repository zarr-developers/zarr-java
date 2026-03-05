package dev.zarr.zarrjava.ome.v1_0;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.ome.OmeV3Group;
import dev.zarr.zarrjava.ome.v1_0.metadata.CollectionMetadata;
import dev.zarr.zarrjava.ome.v1_0.metadata.OmeMetadata;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Group;
import dev.zarr.zarrjava.v3.GroupMetadata;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * OME-Zarr v1.0 (RFC-8) collection backed by a Zarr v3 group.
 */
public final class Collection extends OmeV3Group {

    private OmeMetadata omeMetadata;

    private Collection(
            @Nonnull StoreHandle storeHandle,
            @Nonnull GroupMetadata groupMetadata,
            @Nonnull OmeMetadata omeMetadata
    ) throws IOException {
        super(storeHandle, groupMetadata);
        this.omeMetadata = omeMetadata;
    }

    /**
     * Opens an existing OME-Zarr v1.0 collection at the given store handle.
     */
    public static Collection openCollection(@Nonnull StoreHandle storeHandle) throws IOException, ZarrException {
        Group group = Group.open(storeHandle);
        OmeMetadata omeMetadata = readOmeAttribute(
                group.metadata.attributes, storeHandle, OmeMetadata.class);
        if (omeMetadata.collection == null) {
            throw new ZarrException("v1.0 store at " + storeHandle + " has no 'collection' — is it a MultiscaleImage?");
        }
        return new Collection(storeHandle, group.metadata, omeMetadata);
    }

    /**
     * Creates a new OME-Zarr v1.0 collection at the given store handle.
     */
    public static Collection createCollection(
            @Nonnull StoreHandle storeHandle,
            @Nonnull CollectionMetadata collectionMetadata
    ) throws IOException, ZarrException {
        OmeMetadata omeMetadata = new OmeMetadata("1.0-dev", collectionMetadata);
        Group group = Group.create(storeHandle, omeAttributes(omeMetadata));
        return new Collection(storeHandle, group.metadata, omeMetadata);
    }

    /**
     * Returns the v1.0 collection metadata.
     */
    public CollectionMetadata getCollectionMetadata() {
        return omeMetadata.collection;
    }

    public StoreHandle getStoreHandle() {
        return this.storeHandle;
    }

    /**
     * Opens the child node at the given path, returning either a {@link MultiscaleImage} or a
     * {@link Collection} depending on the metadata present.
     */
    public Object openNode(String path) throws IOException, ZarrException {
        StoreHandle child = storeHandle.resolve(path);
        Group group = Group.open(child);
        OmeMetadata childOme = readOmeAttribute(
                group.metadata.attributes, child, OmeMetadata.class);
        if (childOme.multiscale != null) {
            return MultiscaleImage.openMultiscaleImage(child);
        }
        if (childOme.collection != null) {
            return Collection.openCollection(child);
        }
        throw new ZarrException("Child node at " + child + " has neither 'multiscale' nor 'collection' in ome metadata");
    }
}
