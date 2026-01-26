package dev.zarr.zarrjava.v3;

import com.fasterxml.jackson.databind.ObjectWriter;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.MemoryStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.Utils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;

import static dev.zarr.zarrjava.v3.Node.makeObjectMapper;
import static dev.zarr.zarrjava.v3.Node.makeObjectWriter;


public class Group extends dev.zarr.zarrjava.core.Group implements Node {

    public GroupMetadata metadata;

    protected Group(@Nonnull StoreHandle storeHandle, @Nonnull GroupMetadata groupMetadata) throws IOException {
        super(storeHandle);
        this.metadata = groupMetadata;
    }

    /**
     * Opens an existing Zarr group at a specified storage location.
     *
     * @param storeHandle the storage location of the Zarr group
     * @throws IOException if the metadata cannot be read
     */
    public static Group open(@Nonnull StoreHandle storeHandle) throws IOException {
        StoreHandle metadataHandle = storeHandle.resolve(ZARR_JSON);
        ByteBuffer metadataBytes = metadataHandle.readNonNull();
        return new Group(storeHandle, makeObjectMapper().readValue(Utils.toArray(metadataBytes), GroupMetadata.class));
    }


    /**
     * Opens an existing Zarr group at a specified storage location.
     *
     * @param path the storage location of the Zarr group
     * @throws IOException if the metadata cannot be read
     */
    public static Group open(Path path) throws IOException {
        return open(new StoreHandle(new FilesystemStore(path)));
    }

    /**
     * Opens an existing Zarr group at a specified storage location.
     *
     * @param path the storage location of the Zarr group
     * @throws IOException if the metadata cannot be read
     */
    public static Group open(String path) throws IOException {
        return open(Paths.get(path));
    }

    /**
     * Creates a new Zarr group with default metadata in an in-memory store.
     *
     * @throws IOException if the metadata cannot be serialized
     */
    public static Group create() throws IOException {
        return new Group(new MemoryStore().resolve(), GroupMetadata.defaultValue()).writeMetadata();
    }

    /**
     * Creates a new Zarr group with the provided attributes in an in-memory store.
     *
     * @param attributes the attributes of the Zarr group
     * @throws IOException   if the metadata cannot be serialized
     * @throws ZarrException if the attributes are invalid
     */
    public static Group create(@Nonnull Attributes attributes) throws IOException, ZarrException {
        return new Group(new MemoryStore().resolve(), new GroupMetadata(attributes)).writeMetadata();
    }

    /**
     * Creates a new Zarr group with the provided metadata in an in-memory store.
     *
     * @param groupMetadata the metadata of the Zarr group
     * @throws IOException if the metadata cannot be serialized
     */
    public static Group create(@Nonnull GroupMetadata groupMetadata) throws IOException {
        return new Group(new MemoryStore().resolve(), groupMetadata).writeMetadata();
    }

    /**
     * Creates a new Zarr group with the provided metadata at a specified storage location.
     *
     * @param storeHandle   the storage location of the Zarr group
     * @param groupMetadata the metadata of the Zarr group
     * @throws IOException if the metadata cannot be serialized
     */
    public static Group create(@Nonnull StoreHandle storeHandle, @Nonnull GroupMetadata groupMetadata) throws IOException {
        return new Group(storeHandle, groupMetadata).writeMetadata();
    }

    /**
     * Creates a new Zarr group with the provided attributes at a specified storage location.
     *
     * @param storeHandle the storage location of the Zarr group
     * @param attributes  the attributes of the Zarr group
     * @throws IOException   if the metadata cannot be serialized
     * @throws ZarrException if the attributes are invalid
     */
    public static Group create(
            @Nonnull StoreHandle storeHandle,
            @Nonnull Attributes attributes
    ) throws IOException, ZarrException {
        return create(storeHandle, new GroupMetadata(attributes));
    }

    /**
     * Creates a new Zarr group with default metadata at a specified storage location.
     *
     * @param storeHandle the storage location of the Zarr group
     * @throws IOException if the metadata cannot be serialized
     */
    public static Group create(@Nonnull StoreHandle storeHandle) throws IOException {
        return create(storeHandle, GroupMetadata.defaultValue());
    }

    /**
     * Creates a new Zarr group with the provided metadata at a specified storage location.
     *
     * @param path          the storage location of the Zarr group
     * @param groupMetadata the metadata of the Zarr group
     * @throws IOException   if the metadata cannot be serialized
     * @throws ZarrException if the metadata is invalid
     */
    public static Group create(Path path, GroupMetadata groupMetadata) throws IOException, ZarrException {
        return create(new FilesystemStore(path).resolve(), groupMetadata);
    }

    /**
     * Creates a new Zarr group with the provided metadata at a specified storage location.
     *
     * @param path          the storage location of the Zarr group
     * @param groupMetadata the metadata of the Zarr group
     * @throws IOException   if the metadata cannot be serialized
     * @throws ZarrException if the metadata is invalid
     */
    public static Group create(String path, GroupMetadata groupMetadata) throws IOException, ZarrException {
        return create(Paths.get(path), groupMetadata);
    }

    /**
     * Creates a new Zarr group with default metadata at a specified storage location.
     *
     * @param path the storage location of the Zarr group
     * @throws IOException   if the metadata cannot be serialized
     * @throws ZarrException if the metadata is invalid
     */
    public static Group create(Path path) throws IOException, ZarrException {
        return create(new FilesystemStore(path).resolve());
    }

    /**
     * Creates a new Zarr group with default metadata at a specified storage location.
     *
     * @param path the storage location of the Zarr group
     * @throws IOException   if the metadata cannot be serialized
     * @throws ZarrException if the metadata is invalid
     */
    public static Group create(String path) throws IOException, ZarrException {
        return create(Paths.get(path));
    }

    /**
     * Retrieves a node (group or array) at the specified key within the current group.
     *
     * @param key the key of the node to retrieve
     * @return the node at the specified key, or null if it does not exist
     * @throws ZarrException if the node cannot be opened
     * @throws IOException   if there is an error accessing the storage
     */
    @Nullable
    public Node get(String key) throws ZarrException, IOException {
        StoreHandle keyHandle = storeHandle.resolve(key);
        try {
            return Node.open(keyHandle);
        } catch (NoSuchFileException e) {
            return null;
        }
    }

    /**
     * Creates a new subgroup with the provided metadata at the specified key.
     *
     * @param key           the key of the new Zarr group within the current group
     * @param groupMetadata the metadata of the Zarr group
     * @throws IOException if the metadata cannot be serialized
     */
    public Group createGroup(String key, GroupMetadata groupMetadata)
            throws IOException, ZarrException {
        return Group.create(storeHandle.resolve(key), groupMetadata);
    }

    /**
     * Creates a new subgroup with the provided attributes at the specified key.
     *
     * @param key        the key of the new Zarr group within the current group
     * @param attributes attributes of the Zarr group
     * @throws IOException if the metadata cannot be serialized
     */
    public Group createGroup(String key, Attributes attributes)
            throws IOException, ZarrException {
        return Group.create(storeHandle.resolve(key), new GroupMetadata(attributes));
    }

    /**
     * Creates a new subgroup with default metadata at the specified key.
     *
     * @param key the key of the new Zarr group within the current group
     * @return the created subgroup
     * @throws IOException if the metadata cannot be serialized
     */
    public Group createGroup(String key) throws IOException {
        return Group.create(storeHandle.resolve(key), GroupMetadata.defaultValue());
    }

    /**
     * Creates a new array with the provided metadata at the specified key.
     *
     * @param key           the key of the new Zarr array within the current group
     * @param arrayMetadata the metadata of the Zarr array
     * @return the created array
     * @throws IOException   if the metadata cannot be serialized
     * @throws ZarrException if the array cannot be created
     */
    public Array createArray(String key, ArrayMetadata arrayMetadata)
            throws IOException, ZarrException {
        return Array.create(storeHandle.resolve(key), arrayMetadata);
    }

    /**
     * Creates a new array with the provided metadata builder mapper at the specified key.
     *
     * @param key                        the key of the new Zarr array within the current group
     * @param arrayMetadataBuilderMapper a function building the metadata of the Zarr array
     * @throws IOException if the metadata cannot be serialized
     */
    public Array createArray(String key,
                             Function<ArrayMetadataBuilder, ArrayMetadataBuilder> arrayMetadataBuilderMapper)
            throws IOException, ZarrException {
        return Array.create(storeHandle.resolve(key), arrayMetadataBuilderMapper, false);
    }

    private Group writeMetadata() throws IOException {
        return writeMetadata(this.metadata);
    }

    private Group writeMetadata(GroupMetadata newGroupMetadata) throws IOException {
        ObjectWriter objectWriter = makeObjectWriter();
        ByteBuffer metadataBytes = ByteBuffer.wrap(objectWriter.writeValueAsBytes(newGroupMetadata));
        storeHandle.resolve(ZARR_JSON)
                .set(metadataBytes);
        this.metadata = newGroupMetadata;
        return this;
    }

    /**
     * Updates the attributes of the group using a mapper function.
     *
     * @param attributeMapper a function that takes the current attributes and returns the updated attributes
     * @return the updated group
     * @throws ZarrException if the new attributes are invalid
     * @throws IOException   if the metadata cannot be serialized
     */
    public Group updateAttributes(Function<Attributes, Attributes> attributeMapper)
            throws ZarrException, IOException {
        Attributes currentAttributes = metadata.attributes != null ? new Attributes(metadata.attributes) : new Attributes();
        return setAttributes(attributeMapper.apply(currentAttributes));
    }

    /**
     * Sets new attributes for the group, replacing any existing attributes.
     *
     * @param newAttributes the new attributes to set
     * @return the updated group
     * @throws ZarrException if the new attributes are invalid
     * @throws IOException   if the metadata cannot be serialized
     */
    public Group setAttributes(Attributes newAttributes) throws ZarrException, IOException {
        GroupMetadata newGroupMetadata = new GroupMetadata(newAttributes);
        return writeMetadata(newGroupMetadata);
    }

    @Override
    public String toString() {
        return String.format("<v3.Group {%s}>", storeHandle);
    }

    @Override
    public GroupMetadata metadata() {
        return metadata;
    }
}
