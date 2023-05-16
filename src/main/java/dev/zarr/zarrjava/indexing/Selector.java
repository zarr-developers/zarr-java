package dev.zarr.zarrjava.indexing;

public class Selector {
    public OpenSlice[] value;
    public Selector(int ndim) {
        value = new OpenSlice[ndim];
    }

    public void assertDimensions(int ndim) {
        assert ndim == value.length;
    }

    public StrictSlice[] normalize(long[] shape) {
        assert shape.length == value.length;
        StrictSlice[] output = new StrictSlice[shape.length];
        for (int i = 0; i < shape.length; i++) {
            output[i] = value[i].normalize(shape[i]);
        }
        return output;
    }
}
