package dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import javax.annotation.Nullable;
import java.util.List;

public final class TranslationCoordinateTransformation
        extends dev.zarr.zarrjava.experimental.ome.metadata.transform.TranslationCoordinateTransformation
        implements CoordinateTransformation {

    @Nullable public final String input;
    @Nullable public final String output;
    @Nullable public final String name;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public TranslationCoordinateTransformation(
            @Nullable @JsonProperty("input") @JsonDeserialize(using = CoordinateSystemRefSerde.Deserializer.class) String input,
            @Nullable @JsonProperty("output") @JsonDeserialize(using = CoordinateSystemRefSerde.Deserializer.class) String output,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("translation") List<Double> translation,
            @Nullable @JsonProperty("path") String path
    ) {
        super(translation, path);
        this.input = input;
        this.output = output;
        this.name = name;
    }

    @Override
    public String getType() {
        return this.type;
    }

    @Override
    public String getInput() {
        return input;
    }

    @Override
    public String getOutput() {
        return output;
    }

    @Override
    public String getName() {
        return name;
    }
}
