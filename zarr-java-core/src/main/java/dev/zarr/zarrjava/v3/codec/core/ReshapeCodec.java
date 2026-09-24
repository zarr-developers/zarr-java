package dev.zarr.zarrjava.v3.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.codec.ArrayArrayCodec;
import dev.zarr.zarrjava.v3.ArrayMetadata;
import dev.zarr.zarrjava.v3.codec.Codec;
import ucar.ma2.Array;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * {@code array -> array} codec that reshapes a chunk.
 *
 * <p>The codec preserves the (logical) lexicographical / C-order traversal of the elements, i.e.
 * {@code ravel(B) == ravel(A)}. It never transposes; combine it with the {@code transpose} codec if
 * a reordering is also required.
 *
 * <p>The target {@code shape} is described relative to the input (chunk) shape {@code A_shape}. Each
 * entry is one of:
 * <ul>
 *   <li>a positive integer {@code size}, giving {@code B_shape[i] == size};</li>
 *   <li>an array of input dimension indices {@code input_dims}, giving
 *   {@code B_shape[i] == prod(A_shape[input_dims])};</li>
 *   <li>the special value {@code -1} (at most once), inferred so that
 *   {@code prod(B_shape) == prod(A_shape)}.</li>
 * </ul>
 */
public class ReshapeCodec extends ArrayArrayCodec implements Codec {

    @JsonIgnore
    @Nonnull
    public final String name = "reshape";
    @Nonnull
    public final Configuration configuration;

    /**
     * The resolved (and validated) output chunk shape. It depends only on {@code arrayMetadata.chunkShape}
     * and is therefore computed once in {@link #setCoreArrayMetadata} rather than on every encode/decode.
     */
    @JsonIgnore
    protected int[] outputChunkShape;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public ReshapeCodec(
            @Nonnull @JsonProperty(value = "configuration", required = true) Configuration configuration
    ) {
        this.configuration = configuration;
    }

    @Override
    public void setCoreArrayMetadata(ArrayMetadata.CoreArrayMetadata arrayMetadata) throws ZarrException {
        super.setCoreArrayMetadata(arrayMetadata);
        // Resolve and validate the output shape once, here, because it only changes when the
        // arrayMetadata changes. encode/decode/resolveArrayMetadata then reuse the cached value.
        this.outputChunkShape = resolveOutputShape(arrayMetadata.chunkShape);
    }

    @Override
    public Array encode(Array chunkArray) throws ZarrException {
        int[] inputShape = arrayMetadata.chunkShape;
        if (!Arrays.equals(chunkArray.getShape(), inputShape)) {
            throw new ZarrException(
                    "reshape codec received an array of shape " + Arrays.toString(chunkArray.getShape())
                            + " but expected the chunk shape " + Arrays.toString(inputShape) + ".");
        }
        // Array.reshape copies the elements in lexicographical (C) order, hence ravel(B) == ravel(A)
        // even when the input array is a non-contiguous view.
        return chunkArray.reshape(outputChunkShape);
    }

    @Override
    public Array decode(Array chunkArray) throws ZarrException {
        int[] inputShape = arrayMetadata.chunkShape;
        if (!Arrays.equals(chunkArray.getShape(), outputChunkShape)) {
            throw new ZarrException(
                    "reshape codec received an array of shape " + Arrays.toString(chunkArray.getShape())
                            + " but expected the reshaped shape " + Arrays.toString(outputChunkShape) + ".");
        }
        // Inverse operation: reshape back to the original chunk shape.
        return chunkArray.reshape(inputShape);
    }

    @Override
    public long computeEncodedSize(long inputByteLength,
                                   ArrayMetadata.CoreArrayMetadata arrayMetadata) throws ZarrException {
        // Reshaping does not add or remove any elements.
        return inputByteLength;
    }

    @Override
    public ArrayMetadata.CoreArrayMetadata resolveArrayMetadata() throws ZarrException {
        super.resolveArrayMetadata();

        int[] inputChunkShape = arrayMetadata.chunkShape;

        // Derive the output (grid) array shape so that number of chunks along each output dimension is
        // consistent with the input. For a merged output dimension this is the product of the input
        // chunk counts; a split input dimension keeps its chunk count on the outermost output dimension.
        long[] inputArrayShape = arrayMetadata.shape;
        long[] gridMultiplier = new long[outputChunkShape.length];
        Arrays.fill(gridMultiplier, 1L);

        long[] outputStart = new long[outputChunkShape.length + 1];
        outputStart[0] = 1L;
        for (int i = 0; i < outputChunkShape.length; i++) {
            outputStart[i + 1] = outputStart[i] * outputChunkShape[i];
        }

        long inputStart = 1L;
        for (int d = 0; d < inputChunkShape.length; d++) {
            long numChunks = inputArrayShape[d] / inputChunkShape[d];
            // Attach the chunk count of input dimension d to the output dimension whose flat range
            // contains the start of input dimension d.
            int target = outputChunkShape.length - 1;
            for (int i = 0; i < outputChunkShape.length; i++) {
                if (inputStart >= outputStart[i] && inputStart < outputStart[i + 1]) {
                    target = i;
                    break;
                }
            }
            gridMultiplier[target] *= numChunks;
            inputStart *= inputChunkShape[d];
        }

        long[] outputArrayShape = new long[outputChunkShape.length];
        for (int i = 0; i < outputChunkShape.length; i++) {
            outputArrayShape[i] = gridMultiplier[i] * outputChunkShape[i];
        }

        return new ArrayMetadata.CoreArrayMetadata(
                outputArrayShape,
                outputChunkShape,
                arrayMetadata.dataType,
                arrayMetadata.parsedFillValue
        );
    }

    /**
     * Resolves the concrete output shape {@code B_shape} from the {@code shape} configuration and the
     * given input  shape {@code A_shape}, validating all invariants mandated by the specification.
     */
    int[] resolveOutputShape(int[] inputShape) throws ZarrException {
        long inputTotal = product(inputShape);

        // read the 'shape' config into a concrete (still -1-placeholder) output shape.
        ParsedShape parsed = parseConfiguredShape(inputShape);
        
        // reject configs that reorder input dimensions (use transport for them).
        checkNoReordering(parsed.flatInputDims);
        
        // fill in the single -1 entry, if present, so the element count matches.
        resolveInferredDimension(parsed.outputShape, parsed.minusOnePos, inputTotal);
        
        // The fundamental reshape law, prod(output) == prod(input).
        checkElementCountPreserved(parsed.outputShape, inputTotal);
        
        // Every merge entry must combine adjacent input dims sitting in the same flat position.
        checkMergesAligned(parsed.outputShape, parsed.inputDimsPerOutput, inputShape);
        
        // Narrow long -> int for the array API.
        return toIntShape(parsed.outputShape);
    }

    /**
     * Product of all entries of {@code shape}, computed in {@code long} to avoid overflow.
     */
    private static long product(int[] shape) {
        long total = 1L;
        for (int size : shape) {
            total *= size;
        }
        return total;
    }

    /**
     * Step 1. Reads {@code configuration.shape} into the intermediate {@link ParsedShape}: each entry
     * becomes a literal size, a {@code -1} placeholder, or the product of the merged input dimensions.
     */
    private ParsedShape parseConfiguredShape(int[] inputShape) throws ZarrException {
        int ndim = inputShape.length;
        int n = configuration.shape.length;
        if (n == 0) {
            throw new ZarrException("reshape codec: 'shape' must not be empty.");
        }

        long[] outputShape = new long[n];
        int[][] inputDimsPerOutput = new int[n][];
        int minusOnePos = -1;
        List<Integer> flatInputDims = new ArrayList<>();

        for (int i = 0; i < n; i++) {
            Object element = configuration.shape[i];
            int[] inputDims = asInputDims(element);
            if (inputDims == null) {
                int size = asInteger(element);
                if (size == -1) {
                    if (minusOnePos != -1) {
                        throw new ZarrException("reshape codec: 'shape' may contain -1 at most once.");
                    }
                    minusOnePos = i;
                    outputShape[i] = -1;
                } else if (size <= 0) {
                    throw new ZarrException(
                            "reshape codec: 'shape' entries must be a positive integer, -1, or an array of "
                                    + "input dimensions, but got " + size + ".");
                } else {
                    outputShape[i] = size;
                }
            } else {
                inputDimsPerOutput[i] = inputDims;
                long product = 1L;
                for (int d : inputDims) {
                    if (d < 0 || d >= ndim) {
                        throw new ZarrException(
                                "reshape codec: input dimension " + d + " is out of range for an input array with "
                                        + ndim + " dimensions.");
                    }
                    product *= inputShape[d];
                    flatInputDims.add(d);
                }
                outputShape[i] = product;
            }
        }
        return new ParsedShape(outputShape, inputDimsPerOutput, minusOnePos, flatInputDims);
    }

    /**
     * Step 2. The flattened list of referenced input dimensions must be strictly increasing. This rules
     * out configurations that would (incorrectly) suggest a transpose.
     */
    private static void checkNoReordering(List<Integer> flatInputDims) throws ZarrException {
        for (int j = 1; j < flatInputDims.size(); j++) {
            if (flatInputDims.get(j) <= flatInputDims.get(j - 1)) {
                throw new ZarrException(
                        "reshape codec: the flattened list of input dimensions must be strictly increasing, but got "
                                + flatInputDims + ".");
            }
        }
    }

    /**
     * Step 3. Resolves the automatic dimension ({@code -1}) so that {@code prod(output) == prod(input)}.
     */
    private static void resolveInferredDimension(long[] outputShape, int minusOnePos, long inputTotal)
            throws ZarrException {
        if (minusOnePos == -1) {
            return;
        }
        long known = 1L;
        for (int i = 0; i < outputShape.length; i++) {
            if (i != minusOnePos) {
                known *= outputShape[i];
            }
        }
        if (known == 0 || inputTotal % known != 0) {
            throw new ZarrException(
                    "reshape codec: cannot infer the -1 dimension because prod(output shape) would not equal "
                            + "prod(input shape) (" + inputTotal + ").");
        }
        outputShape[minusOnePos] = inputTotal / known;
    }

    /**
     * Step 4. Invariant: {@code prod(B_shape) == prod(A_shape)}. A reshape never changes the element count.
     */
    private static void checkElementCountPreserved(long[] outputShape, long inputTotal) throws ZarrException {
        long outputTotal = 1L;
        for (long size : outputShape) {
            outputTotal *= size;
        }
        if (outputTotal != inputTotal) {
            throw new ZarrException(
                    "reshape codec: prod(output shape)=" + outputTotal + " does not equal prod(input shape)="
                            + inputTotal + ".");
        }
    }

    /**
     * Step 5. For each output dimension specified by input dimensions, verify that the merged input
     * dimensions actually correspond to the raveled index along that output dimension: the flat range
     * before and after the merged block must match the flat range before and after the output dimension.
     * This is a pure check &mdash; it never modifies {@code outputShape}.
     */
    private static void checkMergesAligned(long[] outputShape, int[][] inputDimsPerOutput, int[] inputShape)
            throws ZarrException {
        int n = outputShape.length;
        int ndim = inputShape.length;
        for (int i = 0; i < n; i++) {
            int[] inputDims = inputDimsPerOutput[i];
            if (inputDims == null || inputDims.length == 0) {
                continue;
            }
            long outputPrefix = 1L;
            for (int j = 0; j < i; j++) {
                outputPrefix *= outputShape[j];
            }
            long outputSuffix = 1L;
            for (int j = i + 1; j < n; j++) {
                outputSuffix *= outputShape[j];
            }
            long inputPrefix = 1L;
            for (int d = 0; d < inputDims[0]; d++) {
                inputPrefix *= inputShape[d];
            }
            long inputSuffix = 1L;
            for (int d = inputDims[inputDims.length - 1] + 1; d < ndim; d++) {
                inputSuffix *= inputShape[d];
            }
            if (outputPrefix != inputPrefix || outputSuffix != inputSuffix) {
                throw new ZarrException(
                        "reshape codec: output dimension " + i + " specified by input dimensions "
                                + Arrays.toString(inputDims) + " does not align with the raveled input array "
                                + "(prefix " + outputPrefix + " vs " + inputPrefix + ", suffix " + outputSuffix
                                + " vs " + inputSuffix + ").");
            }
        }
    }

    /**
     * Step 6. Narrows the resolved {@code long} shape to {@code int[]} (the array API type), rejecting
     * any dimension that would not fit.
     */
    private static int[] toIntShape(long[] outputShape) throws ZarrException {
        int[] result = new int[outputShape.length];
        for (int i = 0; i < outputShape.length; i++) {
            if (outputShape[i] > Integer.MAX_VALUE) {
                throw new ZarrException("reshape codec: output dimension " + i + " exceeds Integer.MAX_VALUE.");
            }
            result[i] = (int) outputShape[i];
        }
        return result;
    }

    /**
     * Intermediate result of {@link #parseConfiguredShape}: the (possibly still {@code -1}-containing)
     * output shape plus the bookkeeping the later validation steps need.
     */
    private static final class ParsedShape {
        /** Output shape; the {@code -1} entry (if any) is still a placeholder until step 3. */
        final long[] outputShape;
        /** Per output dimension, the merged input dims (or {@code null} for a literal/{@code -1} entry). */
        final int[][] inputDimsPerOutput;
        /** Index of the single {@code -1} entry, or {@code -1} if there is none. */
        final int minusOnePos;
        /** All referenced input dimensions, flattened in reading order (used by the no-reorder check). */
        final List<Integer> flatInputDims;

        ParsedShape(long[] outputShape, int[][] inputDimsPerOutput, int minusOnePos,
                    List<Integer> flatInputDims) {
            this.outputShape = outputShape;
            this.inputDimsPerOutput = inputDimsPerOutput;
            this.minusOnePos = minusOnePos;
            this.flatInputDims = flatInputDims;
        }
    }

    private static int[] asInputDims(Object element) throws ZarrException {
        if (element instanceof int[]) {
            return (int[]) element;
        }
        if (element instanceof Object[]) {
            Object[] array = (Object[]) element;
            int[] dims = new int[array.length];
            for (int i = 0; i < array.length; i++) {
                dims[i] = asInteger(array[i]);
            }
            return dims;
        }
        if (element instanceof List) {
            List<?> list = (List<?>) element;
            int[] dims = new int[list.size()];
            for (int i = 0; i < list.size(); i++) {
                dims[i] = asInteger(list.get(i));
            }
            return dims;
        }
        return null;
    }

    private static int asInteger(Object element) throws ZarrException {
        if (element instanceof Number) {
            return ((Number) element).intValue();
        }
        throw new ZarrException(
            "reshape codec: 'shape' entries must be integers or arrays of integers, but got " + element + "."
        );
    }

    public static final class Configuration {
        @Nonnull
        public final Object[] shape;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public Configuration(@Nonnull @JsonProperty(value = "shape", required = true) Object[] shape) {
            this.shape = shape;
        }
    }
}
