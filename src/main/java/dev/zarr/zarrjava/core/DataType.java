package dev.zarr.zarrjava.core;

public interface DataType {
    ucar.ma2.DataType getMA2DataType();

    int getByteCount();

    /**
     * Whether elements are stored as IEEE 754 binary16 but held in memory as {@code float}.
     *
     * <p>For every other data type the encoded element width ({@link #getByteCount()}) matches the
     * width of the in-memory {@link ucar.ma2.DataType}. Half precision is the exception: it encodes
     * to 2 bytes but is held as a 4-byte {@code float}, because neither Java nor {@code ucar.ma2}
     * has a 16-bit floating point type. The {@code bytes} codec uses this to convert at the
     * encode/decode boundary via {@link dev.zarr.zarrjava.utils.Float16}.
     *
     * @return true for {@code float16}, false for every other data type
     */
    default boolean isHalfPrecisionFloat() {
        return false;
    }
}
