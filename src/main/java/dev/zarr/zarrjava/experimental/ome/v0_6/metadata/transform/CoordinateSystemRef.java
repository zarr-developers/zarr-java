package dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;

/**
 * Reference to a coordinate system, used as the {@code input} / {@code output} of an OME-Zarr v0.6
 * coordinate transformation.
 *
 * <p>Serialized as a JSON object {@code {"name": ..., "path": ...}}; both fields are optional and
 * omitted when {@code null}. Which fields are required depends on the context
 * (see the OME-Zarr 0.6 "coordinateTransformations" constraints):
 * <ul>
 *   <li>{@code multiscales > datasets}: input {@code {"path": "<dataset path>"}},
 *       output {@code {"name": "<intrinsic coordinate system>"}}</li>
 *   <li>{@code multiscales > coordinateTransformations}: {@code {"name": ...}}, optionally with a
 *       {@code path} to a child labels group</li>
 *   <li>{@code scene > coordinateTransformations}: {@code {"name": ..., "path": "<image group>"}}</li>
 * </ul>
 * A {@code null} or omitted {@code path} refers to a coordinate system in the same {@code zarr.json}.
 *
 * <p><b>Legacy string form.</b> Pre-release 0.6 drafts stored references as plain strings. For
 * backwards compatibility a bare JSON string is still accepted on read and interpreted as a
 * coordinate system {@code name}, except for the {@code input} of a top-level
 * {@code multiscales > datasets} transformation, where it is interpreted as the dataset
 * {@code path} (see {@link dev.zarr.zarrjava.experimental.ome.v0_6.metadata.Dataset}).
 * Strings of the form {@code "<path>#<name>"} (as written by earlier zarr-java versions) are split
 * into path and name, with {@code "."} meaning "no path". Writing always emits the object form.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize(using = CoordinateSystemRef.Deserializer.class)
public final class CoordinateSystemRef {

    @Nullable
    @JsonProperty("name")
    public final String name;
    @Nullable
    @JsonProperty("path")
    public final String path;

    public CoordinateSystemRef(@Nullable String name, @Nullable String path) {
        this.name = name;
        this.path = path;
    }

    /** Reference to a named coordinate system in the same {@code zarr.json}. */
    public static CoordinateSystemRef ofName(String name) {
        return new CoordinateSystemRef(name, null);
    }

    /** Reference by path only (e.g. the input of a dataset transformation: the array at {@code path}). */
    public static CoordinateSystemRef ofPath(String path) {
        return new CoordinateSystemRef(null, path);
    }

    /** Reference to the coordinate system {@code name} defined in the Zarr group at {@code path}. */
    public static CoordinateSystemRef of(@Nullable String name, @Nullable String path) {
        return new CoordinateSystemRef(name, path);
    }

    /**
     * Parses the legacy (pre-release 0.6) string form of a reference: a bare string is a name;
     * {@code "<path>#<name>"} is split into path and name ({@code "."} or empty path means no path).
     */
    public static CoordinateSystemRef fromLegacyString(String value) {
        int hash = value.indexOf('#');
        if (hash < 0) {
            return ofName(value);
        }
        String p = value.substring(0, hash);
        String n = value.substring(hash + 1);
        if (p.isEmpty() || ".".equals(p)) {
            p = null;
        }
        return new CoordinateSystemRef(n.isEmpty() ? null : n, p);
    }

    @Nullable
    public String getName() {
        return name;
    }

    @Nullable
    public String getPath() {
        return path;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CoordinateSystemRef)) {
            return false;
        }
        CoordinateSystemRef that = (CoordinateSystemRef) o;
        return Objects.equals(name, that.name) && Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, path);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        if (name != null) {
            sb.append("name=").append(name);
        }
        if (path != null) {
            if (name != null) {
                sb.append(", ");
            }
            sb.append("path=").append(path);
        }
        return sb.append('}').toString();
    }

    static final class Deserializer extends StdDeserializer<CoordinateSystemRef> {
        Deserializer() {
            super(CoordinateSystemRef.class);
        }

        @Override
        public CoordinateSystemRef deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            JsonToken token = p.currentToken();
            if (token == JsonToken.VALUE_STRING) {
                return fromLegacyString(p.getValueAsString());
            }
            if (token == JsonToken.START_OBJECT) {
                JsonNode node = p.readValueAsTree();
                return new CoordinateSystemRef(textOrNull(node.get("name")), textOrNull(node.get("path")));
            }
            return (CoordinateSystemRef) ctxt.handleUnexpectedToken(CoordinateSystemRef.class, p);
        }

        @Nullable
        private static String textOrNull(@Nullable JsonNode node) {
            return node != null && node.isTextual() ? node.asText() : null;
        }
    }
}
