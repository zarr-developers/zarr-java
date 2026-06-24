package dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform;

import javax.annotation.Nullable;

abstract class BaseCoordinateTransformation implements CoordinateTransformation {
    public final String type;
    @Nullable public final String input;
    @Nullable public final String output;
    @Nullable public final String name;

    protected BaseCoordinateTransformation(
            String type,
            @Nullable String input,
            @Nullable String output,
            @Nullable String name
    ) {
        this.type = type;
        this.input = input;
        this.output = output;
        this.name = name;
    }

    @Override
    public String getType() {
        return type;
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
