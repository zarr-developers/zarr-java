package dev.zarr.zarrjava.v3;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static dev.zarr.zarrjava.v3.Node.makeObjectMapper;
import static dev.zarr.zarrjava.v3.Node.makeObjectWriter;


public class Group extends dev.zarr.zarrjava.core.Group implements Node {

    private static final Logger LOGGER = Logger.getLogger(Group.class.getName());

    /**
     * The order in which entries are written into the consolidated metadata: shallow paths first,
     * then case-insensitively by name. This only affects the byte layout of the written metadata
     * document, but it makes consolidating the same hierarchy twice produce an identical file.
     */
    private static final Comparator<String> CONSOLIDATED_KEY_ORDER = Comparator
            .comparingInt((String key) -> (int) key.chars().filter(c -> c == '/').count())
            .thenComparing(key -> Normalizer.normalize(key, Normalizer.Form.NFKC).toLowerCase(Locale.ROOT))
            .thenComparing(Comparator.naturalOrder());

    public GroupMetadata metadata;

    /**
     * Whether {@link #get} may be answered from the consolidated metadata of this group.
     */
    private final boolean useConsolidated;

    protected Group(@Nonnull StoreHandle storeHandle, @Nonnull GroupMetadata groupMetadata) throws IOException {
        this(storeHandle, groupMetadata, true);
    }

    protected Group(@Nonnull StoreHandle storeHandle, @Nonnull GroupMetadata groupMetadata,
                    boolean useConsolidated) throws IOException {
        super(storeHandle);
        this.metadata = groupMetadata;
        this.useConsolidated = useConsolidated;
    }

    /**
     * Opens an existing Zarr group at a specified storage location.
     *
     * @param storeHandle the storage location of the Zarr group
     * @throws IOException if the metadata cannot be read
     */
    public static Group open(@Nonnull StoreHandle storeHandle) throws IOException {
        return open(storeHandle, true);
    }

    /**
     * Opens an existing Zarr group at a specified storage location.
     *
     * @param storeHandle     the storage location of the Zarr group
     * @param useConsolidated whether the consolidated metadata of the group, if it has any, may be
     *                        used to look up its descendants. Pass false to always read every node
     *                        from the store, for example when the hierarchy may have been modified
     *                        since it was consolidated.
     * @throws IOException if the metadata cannot be read
     */
    public static Group open(@Nonnull StoreHandle storeHandle, boolean useConsolidated) throws IOException {
        StoreHandle metadataHandle = storeHandle.resolve(ZARR_JSON);
        ByteBuffer metadataBytes = metadataHandle.readNonNull();
        GroupMetadata groupMetadata =
                makeObjectMapper().readValue(Utils.toArray(metadataBytes), GroupMetadata.class);
        return new Group(storeHandle, groupMetadata, useConsolidated);
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
    public static Group create(@Nonnull StoreHandle storeHandle, @Nonnull Attributes attributes) throws IOException, ZarrException {
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
    public Node get(String[] key) throws ZarrException, IOException {
        ConsolidatedMetadata consolidated = useConsolidated ? metadata.consolidatedMetadata : null;
        if (consolidated == null || !consolidated.isInline()) {
            return openFromStore(key);
        }
        JsonNode cached = consolidated.get(key);
        if (cached != null) {
            Node node = nodeFromConsolidatedMetadata(key, cached, consolidated);
            if (node != null) {
                return node;
            }
            // The cached document could not be interpreted, fall back to the node itself.
            return openFromStore(key);
        }
        Node node = openFromStore(key);
        if (node != null) {
            LOGGER.warning("The node '" + String.join("/", key) + "' below " + storeHandle
                    + " is missing from the consolidated metadata of the group. The consolidated"
                    + " metadata is a snapshot and does not track later changes to the hierarchy;"
                    + " call consolidateMetadata() again to refresh it.");
        }
        return node;
    }

    /**
     * Opens the node at {@code key} by reading its metadata from the store, ignoring any consolidated
     * metadata.
     */
    @Nullable
    private Node openFromStore(String[] key) throws ZarrException, IOException {
        StoreHandle keyHandle = storeHandle.resolve(key);
        try {
            Node node = Node.open(keyHandle);
            if (!useConsolidated && node instanceof Group) {
                Group group = (Group) node;
                return new Group(group.storeHandle, group.metadata, false);
            }
            return node;
        } catch (NoSuchFileException e) {
            return null;
        }
    }

    @Override
    public Stream<dev.zarr.zarrjava.core.Node> list() {
        Stream<String[]> metadataKeys = storeHandle.list()
                .filter(key -> key[key.length - 1].equals(ZARR_JSON))
                .filter(key -> key.length > 1); // exclude root from list
        return metadataKeys.map(key -> {
            try {
                return get(Arrays.copyOf(key, key.length - 1));
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to read node metadata for key '" + String.join("/", key) + "': " + e.getMessage(), e);
            } catch (ZarrException e) {
                throw new RuntimeException(
                        "Failed to parse node metadata for key '" + String.join("/", key) + "': " + e.getMessage(), e);
            }
        });
    }


    /**
     * Builds a node from a cached metadata document, or returns null if the document cannot be
     * interpreted. The consolidated metadata is declared with {@code must_understand: false}, so an
     * entry this library does not understand is skipped in favour of reading the node itself rather
     * than failing.
     */
    @Nullable
    private Node nodeFromConsolidatedMetadata(String[] key, JsonNode cached,
                                              ConsolidatedMetadata consolidated) {
        StoreHandle keyHandle = storeHandle.resolve(key);
        JsonNode nodeTypeNode = cached.get("node_type");
        String nodeType = nodeTypeNode == null ? null : nodeTypeNode.asText();
        try {
            ObjectMapper objectMapper = makeObjectMapper();
            if (ArrayMetadata.NODE_TYPE.equals(nodeType)) {
                return new Array(keyHandle, objectMapper.treeToValue(cached, ArrayMetadata.class));
            }
            if (GroupMetadata.NODE_TYPE.equals(nodeType)) {
                GroupMetadata groupMetadata = objectMapper.treeToValue(cached, GroupMetadata.class);
                // The entries of a consolidated subgroup are hoisted into the cache of this group, so
                // hand the subgroup its own slice of them instead of the emptied cache it carries.
                return new Group(keyHandle,
                        groupMetadata.withConsolidatedMetadata(consolidated.sub(key)), true);
            }
            LOGGER.warning("Ignoring the consolidated metadata of '" + String.join("/", key)
                    + "' below " + storeHandle + ", it has an unsupported node type '" + nodeType + "'.");
            return null;
        } catch (Exception e) {
            LOGGER.warning("Ignoring the consolidated metadata of '" + String.join("/", key)
                    + "' below " + storeHandle + ", it could not be parsed: " + e.getMessage());
            return null;
        }
    }

    /**
     * Writes the metadata of all descendants of this group into the metadata of this group, so that
     * the whole hierarchy can afterwards be opened with a single read.
     * <p>
     * The metadata of each descendant is copied verbatim, keyed by its {@code "/"}-joined path
     * relative to this group. The copy of a subgroup is given an empty cache of its own, marking it as
     * covered by the cache written here; a subgroup that was consolidated itself therefore does not
     * have its entries stored twice.
     * <p>
     * The result is a snapshot. Nothing invalidates it when a node is added, removed or modified
     * afterwards, so this has to be called again after changing the hierarchy. Reading a node that is
     * missing from the cache logs a warning and falls back to reading the node itself, but a node that
     * was modified after consolidating is served from the cache and cannot be detected.
     *
     * @return this group, with the consolidated metadata written
     * @throws IOException                   if the metadata cannot be read or written
     * @throws UnsupportedOperationException if the underlying store does not support listing
     */
    public Group consolidateMetadata() throws IOException {
        Map<String, JsonNode> entries = new LinkedHashMap<>();
        collectDescendantMetadata(new String[0], entries);

        List<String> keys = new ArrayList<>(entries.keySet());
        keys.sort(CONSOLIDATED_KEY_ORDER);
        Map<String, JsonNode> sorted = new LinkedHashMap<>();
        for (String key : keys) {
            sorted.put(key, entries.get(key));
        }
        return writeMetadata(
                metadata.withConsolidatedMetadata(new ConsolidatedMetadata(sorted)));
    }

    /**
     * Removes the consolidated metadata of this group, so that its descendants are read from the store
     * again.
     *
     * @return this group, with the consolidated metadata removed
     * @throws IOException if the metadata cannot be written
     */
    public Group dropConsolidatedMetadata() throws IOException {
        if (metadata.consolidatedMetadata == null) {
            return this;
        }
        return writeMetadata(metadata.withConsolidatedMetadata(null));
    }

    /**
     * Collects the metadata documents of all nodes below {@code prefix} into {@code out}, keyed by
     * their path relative to this group.
     */
    private void collectDescendantMetadata(String[] prefix, Map<String, JsonNode> out)
            throws IOException {
        List<String> children;
        try (Stream<String> stream = storeHandle.resolve(prefix).listChildren()) {
            children = stream.filter(name -> !ZARR_JSON.equals(name)).collect(Collectors.toList());
        }
        for (String child : children) {
            String[] key = Utils.concatArrays(prefix, new String[]{child});
            ByteBuffer metadataBytes = storeHandle.resolve(key).resolve(ZARR_JSON).read();
            if (metadataBytes == null) {
                // Not a node itself, but it may still contain nodes further down.
                collectDescendantMetadata(key, out);
                continue;
            }
            JsonNode nodeMetadata = makeObjectMapper().readTree(Utils.toArray(metadataBytes));
            JsonNode nodeTypeNode = nodeMetadata.get("node_type");
            boolean isGroup = nodeTypeNode != null && GroupMetadata.NODE_TYPE.equals(nodeTypeNode.asText());
            if (isGroup) {
                markSubgroupAsConsolidated(nodeMetadata);
            }
            out.put(String.join("/", key), nodeMetadata);
            if (isGroup) {
                collectDescendantMetadata(key, out);
            }
        }
    }

    /**
     * Gives the cached metadata of a subgroup an empty consolidated metadata cache of its own. The
     * empty cache marks the subgroup as covered by the cache being written here, which is where its
     * entries live. A subgroup that carried a cache of its own loses it in this copy, so that the same
     * entries are not held twice and cannot drift apart. This mirrors what zarr-python writes.
     */
    private static void markSubgroupAsConsolidated(JsonNode nodeMetadata) {
        if (!(nodeMetadata instanceof ObjectNode)) {
            return;
        }
        ObjectNode metadataObject = (ObjectNode) nodeMetadata;
        ObjectNode nested = metadataObject.objectNode();
        nested.put("kind", ConsolidatedMetadata.KIND_INLINE);
        nested.put("must_understand", false);
        nested.set("metadata", metadataObject.objectNode());
        metadataObject.set("consolidated_metadata", nested);
    }

    /**
     * Creates a new subgroup with the provided metadata at the specified key.
     *
     * @param key           the key of the new Zarr group within the current group
     * @param groupMetadata the metadata of the Zarr group
     * @throws IOException if the metadata cannot be serialized
     */
    public Group createGroup(String key, GroupMetadata groupMetadata) throws IOException, ZarrException {
        return Group.create(storeHandle.resolve(key), groupMetadata);
    }

    /**
     * Creates a new subgroup with the provided attributes at the specified key.
     *
     * @param key        the key of the new Zarr group within the current group
     * @param attributes attributes of the Zarr group
     * @throws IOException if the metadata cannot be serialized
     */
    public Group createGroup(String key, Attributes attributes) throws IOException, ZarrException {
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
    public Array createArray(String key, ArrayMetadata arrayMetadata) throws IOException, ZarrException {
        return Array.create(storeHandle.resolve(key), arrayMetadata);
    }

    /**
     * Creates a new array with the provided metadata builder mapper at the specified key.
     *
     * @param key                        the key of the new Zarr array within the current group
     * @param arrayMetadataBuilderMapper a function building the metadata of the Zarr array
     * @throws IOException if the metadata cannot be serialized
     */
    public Array createArray(String key, Function<ArrayMetadataBuilder, ArrayMetadataBuilder> arrayMetadataBuilderMapper) throws IOException, ZarrException {
        return Array.create(storeHandle.resolve(key), arrayMetadataBuilderMapper, false);
    }

    private Group writeMetadata() throws IOException {
        return writeMetadata(this.metadata);
    }

    private Group writeMetadata(GroupMetadata newGroupMetadata) throws IOException {
        ObjectWriter objectWriter = makeObjectWriter();
        ByteBuffer metadataBytes = ByteBuffer.wrap(objectWriter.writeValueAsBytes(newGroupMetadata));
        storeHandle.resolve(ZARR_JSON).set(metadataBytes);
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
    public Group updateAttributes(Function<Attributes, Attributes> attributeMapper) throws ZarrException, IOException {
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
        // The consolidated metadata describes the descendants of this group, which are unaffected by
        // a change to the attributes of the group itself.
        GroupMetadata newGroupMetadata =
                new GroupMetadata(newAttributes, metadata.consolidatedMetadata);
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
