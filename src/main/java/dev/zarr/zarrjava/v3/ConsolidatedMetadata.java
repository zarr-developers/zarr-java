package dev.zarr.zarrjava.v3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An optional cache of the metadata of all descendants of a group, stored inside that group's own
 * {@code zarr.json} under the {@code consolidated_metadata} key. It allows a reader to open a whole
 * hierarchy with a single request instead of one request per node.
 * <p>
 * The keys of {@link #metadata} are flat, {@code "/"}-joined paths relative to the group holding the
 * cache, for example {@code "ocean"} and {@code "ocean/salinity"}.
 * <p>
 * A cache of a kind other than {@link #KIND_INLINE} makes the group unopenable, which is what
 * zarr-python does as well.
 * <p>
 * The cached node metadata is deliberately kept as raw {@link JsonNode} rather than as parsed
 * {@link ArrayMetadata} / {@link GroupMetadata}. The cache is declared with
 * {@code must_understand: false}, so a reader that cannot interpret an entry has to ignore it rather
 * than fail. Parsing entries eagerly would mean that a single node written by another implementation
 * with a field this library does not model would make the whole group unopenable. Keeping the raw
 * JSON also lets {@link Group#consolidateMetadata()} copy each node's metadata verbatim, so the cache
 * never silently loses information that is present in the node's own {@code zarr.json}.
 * <p>
 * The cache is a snapshot taken at the time of consolidation. Nothing invalidates it when a
 * descendant changes, so {@link Group#consolidateMetadata()} has to be re-run after modifying the
 * hierarchy.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ConsolidatedMetadata {

    /**
     * The only cache kind defined so far: the metadata is stored inline in the group's metadata
     * document. A cache of any other kind is rejected by this library.
     */
    public static final String KIND_INLINE = "inline";

    @Nonnull
    @JsonProperty("kind")
    public final String kind;

    @JsonProperty("must_understand")
    public final boolean mustUnderstand;

    /**
     * The cached metadata documents, keyed by their {@code "/"}-joined path relative to the group
     * holding this cache.
     */
    @Nonnull
    @JsonProperty("metadata")
    public final Map<String, JsonNode> metadata;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public ConsolidatedMetadata(
            @Nullable @JsonProperty("kind") String kind,
            @Nullable @JsonProperty("must_understand") Boolean mustUnderstand,
            @Nullable @JsonProperty("metadata") Map<String, JsonNode> metadata
    ) {
        this.kind = kind == null ? KIND_INLINE : kind;
        this.mustUnderstand = mustUnderstand != null && mustUnderstand;
        this.metadata = metadata == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(metadata));
    }

    public ConsolidatedMetadata(@Nonnull Map<String, JsonNode> metadata) {
        this(KIND_INLINE, false, metadata);
    }

    /**
     * An empty inline cache, used for a consolidated subgroup whose own entries have been hoisted
     * into the cache of an ancestor.
     */
    public static ConsolidatedMetadata empty() {
        return new ConsolidatedMetadata(Collections.emptyMap());
    }

    /**
     * Whether this cache is stored inline and can therefore be used by this library. A group whose
     * cache is not inline cannot be opened, see {@link GroupMetadata}.
     */
    @JsonIgnore
    public boolean isInline() {
        return KIND_INLINE.equals(kind);
    }

    @JsonIgnore
    public boolean isEmpty() {
        return metadata.isEmpty();
    }

    /**
     * Returns the cached metadata document for a node, or null if this cache does not hold it.
     *
     * @param key the path of the node relative to the group holding this cache
     */
    @Nullable
    public JsonNode get(String[] key) {
        if (!isInline()) {
            return null;
        }
        return metadata.get(String.join("/", key));
    }

    /**
     * Returns the keys of this cache with every group listed before its own descendants, and
     * siblings in the order they are held in this cache. This is the order in which zarr-python
     * yields the members of a consolidated group, because it walks the nested representation it
     * builds when reading the cache.
     */
    public List<String> depthFirstKeys() {
        Map<String, List<String>> childrenByParent = new LinkedHashMap<>();
        for (String key : metadata.keySet()) {
            int lastSlash = key.lastIndexOf('/');
            String parent = lastSlash < 0 ? "" : key.substring(0, lastSlash);
            childrenByParent.computeIfAbsent(parent, unused -> new ArrayList<>()).add(key);
        }
        Set<String> ordered = new LinkedHashSet<>();
        appendDepthFirst("", childrenByParent, ordered);
        // A key whose parent is missing from the cache is not reachable from the group, so it is not
        // visited above. Append it, so that a malformed cache hides nothing.
        ordered.addAll(metadata.keySet());
        return new ArrayList<>(ordered);
    }

    private static void appendDepthFirst(String parent, Map<String, List<String>> childrenByParent,
                                        Set<String> out) {
        List<String> children = childrenByParent.get(parent);
        if (children == null) {
            return;
        }
        for (String child : children) {
            out.add(child);
            appendDepthFirst(child, childrenByParent, out);
        }
    }

    /**
     * Returns the entries below {@code prefix} with the prefix stripped from their keys, so that the
     * result can serve as the cache of the subgroup at {@code prefix}.
     */
    public ConsolidatedMetadata sub(String[] prefix) {
        if (!isInline()) {
            return empty();
        }
        String keyPrefix = String.join("/", prefix) + "/";
        Map<String, JsonNode> sub = new LinkedHashMap<>();
        for (Map.Entry<String, JsonNode> entry : metadata.entrySet()) {
            if (entry.getKey().startsWith(keyPrefix)) {
                sub.put(entry.getKey().substring(keyPrefix.length()), entry.getValue());
            }
        }
        return new ConsolidatedMetadata(sub);
    }
}
