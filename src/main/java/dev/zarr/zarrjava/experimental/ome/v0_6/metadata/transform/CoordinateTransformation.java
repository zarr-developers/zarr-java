package dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "type",
        visible = true,
        defaultImpl = GenericCoordinateTransformation.class)
@JsonSubTypes({
        @JsonSubTypes.Type(value = ScaleCoordinateTransformation.class, name = "scale"),
        @JsonSubTypes.Type(value = TranslationCoordinateTransformation.class, name = "translation"),
        @JsonSubTypes.Type(value = IdentityCoordinateTransformation.class, name = "identity"),
        @JsonSubTypes.Type(value = SequenceCoordinateTransformation.class, name = "sequence"),
        @JsonSubTypes.Type(value = MapAxisCoordinateTransformation.class, name = "mapAxis"),
        @JsonSubTypes.Type(value = AffineCoordinateTransformation.class, name = "affine"),
        @JsonSubTypes.Type(value = RotationCoordinateTransformation.class, name = "rotation"),
        @JsonSubTypes.Type(value = DisplacementsCoordinateTransformation.class, name = "displacements"),
        @JsonSubTypes.Type(value = CoordinatesCoordinateTransformation.class, name = "coordinates"),
        @JsonSubTypes.Type(value = BijectionCoordinateTransformation.class, name = "bijection"),
        @JsonSubTypes.Type(value = ByDimensionCoordinateTransformation.class, name = "byDimension")
})
public abstract class CoordinateTransformation {

    public final String type;
    @Nullable
    @JsonDeserialize(using = CoordinateSystemRefDeserializer.class)
    @JsonSerialize(using = CoordinateSystemRefSerializer.class)
    public final String input;
    @Nullable
    @JsonDeserialize(using = CoordinateSystemRefDeserializer.class)
    @JsonSerialize(using = CoordinateSystemRefSerializer.class)
    public final String output;
    @Nullable public final String name;

    protected CoordinateTransformation(
            @JsonProperty(value = "type", required = true) String type,
            @Nullable @JsonProperty("input") String input,
            @Nullable @JsonProperty("output") String output,
            @Nullable @JsonProperty("name") String name
    ) {
        this.type = type;
        this.input = input;
        this.output = output;
        this.name = name;
    }

    public static CoordinateTransformation scale(List<Double> scale, String input, String output) {
        return new ScaleCoordinateTransformation(input, output, null, scale, null);
    }

    public static CoordinateTransformation translation(List<Double> translation, String input, String output) {
        return new TranslationCoordinateTransformation(input, output, null, translation, null);
    }

    public static CoordinateTransformation identity(String input, String output) {
        return new IdentityCoordinateTransformation(input, output, null, null);
    }

    public static CoordinateTransformation sequence(List<CoordinateTransformation> transformations, String input, String output) {
        return new SequenceCoordinateTransformation(input, output, null, transformations);
    }

    public static final class CoordinateSystemRefDeserializer extends JsonDeserializer<String> {
        @Override
        public String deserialize(JsonParser parser, DeserializationContext context) throws IOException {
            if (parser.currentToken() == JsonToken.VALUE_STRING) {
                return parser.getValueAsString();
            }
            if (parser.currentToken() == JsonToken.START_OBJECT) {
                String path = null;
                String name = null;
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    String field = parser.getCurrentName();
                    parser.nextToken();
                    if ("path".equals(field)) {
                        path = parser.getValueAsString();
                    } else if ("name".equals(field)) {
                        name = parser.getValueAsString();
                    } else {
                        parser.skipChildren();
                    }
                }
                if (name == null || name.isEmpty()) {
                    return null;
                }
                String prefix = (path == null || path.isEmpty()) ? "." : path;
                return prefix + "#" + name;
            }
            if (parser.currentToken() == JsonToken.VALUE_NULL) {
                return null;
            }
            return parser.getValueAsString();
        }
    }

    public static final class CoordinateSystemRefSerializer extends JsonSerializer<String> {
        @Override
        public void serialize(String value, JsonGenerator generator, SerializerProvider serializers) throws IOException {
            if (value == null) {
                generator.writeNull();
                return;
            }
            int split = value.indexOf('#');
            if (split <= 0 || split >= value.length() - 1) {
                generator.writeString(value);
                return;
            }
            String path = value.substring(0, split);
            String name = value.substring(split + 1);
            generator.writeStartObject();
            if (!".".equals(path)) {
                generator.writeStringField("path", path);
            }
            generator.writeStringField("name", name);
            generator.writeEndObject();
        }
    }

}
