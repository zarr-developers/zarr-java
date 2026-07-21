package dev.zarr.zarrjava.v3.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.codec.ArrayBytesCodec;
import dev.zarr.zarrjava.utils.Utils;
import dev.zarr.zarrjava.v3.ArrayMetadata;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.codec.Codec;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import ucar.ma2.Array;

import javax.annotation.Nullable;
import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.metadata.IIOMetadataNode;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageOutputStream;
import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Encodes a {@code uint8} chunk as a baseline (sequential DCT, Huffman-coded) JPEG image,
 * compatible with neuroglancer's {@code precomputed} "jpeg" chunk encoding.
 *
 * <p>The component count is derived strictly from the chunk shape: a 2D shape {@code (H, W)} or a
 * 3D shape {@code (H, W, 1)} is grayscale (1 component); a 3D shape {@code (H, W, 3)} is color
 * (3 components). Any other shape is rejected — reshape or transpose the data with a preceding
 * codec so the (single or triple) channel axis is innermost. The chunk is read in C order, so the
 * trailing spatial axes give the image height {@code H} and width {@code W}, each limited to
 * 65535 by the JPEG format. This codec never reshapes or resamples the data itself.
 *
 * <p>For 3-component data the {@code encoded_color_space} is required and selects how the samples
 * are stored: {@code ycbcr} converts RGB to YCbCr (the JFIF color space, enabling chroma
 * subsampling), while {@code rgb} stores the three components without conversion and writes an
 * APP14 Adobe marker with an "unknown" transform so decoders do not apply an inverse YCbCr
 * transform. The inverse transform on decode is determined by the stream's markers, not by the
 * configuration.
 *
 * <p>JPEG is lossy, so this codec must not be used where exact values matter (e.g. segmentation).
 */
public class JpegCodec extends ArrayBytesCodec implements Codec {

    /** JPEG images may not exceed this size along either dimension (stored as a 16-bit value). */
    private static final int MAX_JPEG_DIMENSION = 65535;

    /** APP14 Adobe color transform code for "unknown" (i.e. no YCbCr transform). */
    private static final int ADOBE_TRANSFORM_UNKNOWN = 0;
    /** APP14 Adobe color transform code for YCbCr. */
    private static final int ADOBE_TRANSFORM_YCBCR = 1;

    @JsonIgnore
    public final String name = "jpeg";
    @Nullable
    public final Configuration configuration;

    @JsonCreator
    public JpegCodec(
            @JsonProperty(value = "configuration") Configuration configuration) {
        this.configuration = configuration;
    }

    public JpegCodec() {
        this((Configuration) null);
    }

    public JpegCodec(int quality) throws ZarrException {
        this(new Configuration(quality));
    }

    public JpegCodec(int quality, @Nullable String encodedColorSpace, @Nullable String subsampling)
            throws ZarrException {
        this(new Configuration(quality, encodedColorSpace, subsampling));
    }

    private int getQualityInternal() {
        return configuration == null ? Configuration.DEFAULT_QUALITY : configuration.quality;
    }

    @Nullable
    private String getEncodedColorSpace() {
        return configuration == null ? null : configuration.encodedColorSpace;
    }

    @Nullable
    private int[][] getSubsampling() {
        return configuration == null ? null : configuration.subsampling;
    }

    /**
     * Derive the JPEG component count (1 or 3) from the chunk shape, rejecting any shape that JPEG
     * cannot represent. {@code (H, W)} and {@code (H, W, 1)} are grayscale; {@code (H, W, 3)} is
     * color; everything else (1D, an unsupported channel count, or 4D and higher) is an error.
     */
    private int determineNumChannels() throws ZarrException {
        int[] shape = arrayMetadata.chunkShape;
        if (shape.length == 2) {
            return 1;
        }
        if (shape.length == 3) {
            if (shape[2] == 1) {
                return 1;
            }
            if (shape[2] == 3) {
                return 3;
            }
        }
        throw new ZarrException(
                "The jpeg codec only supports chunk shapes (H, W), (H, W, 1) or (H, W, 3), got "
                        + Arrays.toString(shape)
                        + ". Reshape or transpose the data so the channel axis (of extent 1 or 3) is "
                        + "innermost before the jpeg codec.");
    }

    /**
     * Validate that the configured color-space parameters are consistent with the chunk's channel
     * count, per the codec specification. This rejects invalid configurations rather than silently
     * ignoring them.
     */
    private void validateConfiguration(int numChannels) throws ZarrException {
        String encodedColorSpace = getEncodedColorSpace();
        int[][] subsampling = getSubsampling();
        if (numChannels == 1) {
            if (encodedColorSpace != null) {
                throw new ZarrException(
                        "'encoded_color_space' must not be set for grayscale (1-component) data.");
            }
            if (subsampling != null) {
                throw new ZarrException(
                        "'subsampling' must not be set for grayscale (1-component) data.");
            }
        } else {
            if (encodedColorSpace == null) {
                throw new ZarrException(
                        "'encoded_color_space' is required for 3-component data; set it to \"ycbcr\" "
                                + "(natural color images) or \"rgb\" (independent scientific channels).");
            }
            if (subsampling != null && subsampling.length != numChannels) {
                throw new ZarrException(
                        "'subsampling' must have one entry per component (" + numChannels + "), got "
                                + subsampling.length + ".");
            }
        }
    }

    /**
     * Resolve the luma component's sampling factors ({@code [horizontal, vertical]}) from the
     * configured subsampling, which is expressed as per-component JPEG sampling factors (chroma is
     * always {@code [1, 1]}). Defaults to {@code [2, 2]} (the 4:2:0 scheme) when unset.
     */
    private int[] lumaSamplingFactors() {
        int[][] subsampling = getSubsampling();
        if (subsampling == null) {
            return Configuration.DEFAULT_LUMA_SAMPLING.clone();
        }
        return new int[]{subsampling[0][0], subsampling[0][1]};
    }

    @Override
    public ByteBuffer encode(Array chunkArray) throws ZarrException {
        if (arrayMetadata.dataType != DataType.UINT8) {
            throw new ZarrException(
                    "The jpeg codec only supports the uint8 data type, got " + arrayMetadata.dataType + ".");
        }
        int numChannels = determineNumChannels();
        validateConfiguration(numChannels);

        int[] shape = arrayMetadata.chunkShape;
        int height = shape[0];
        int width = shape[1];
        if (height > MAX_JPEG_DIMENSION || width > MAX_JPEG_DIMENSION) {
            throw new ZarrException(
                    "JPEG image dimensions must not exceed " + MAX_JPEG_DIMENSION + ", got height="
                            + height + ", width=" + width + ".");
        }

        int totalElements = (int) chunkArray.getSize();
        ByteBuffer src = chunkArray.getDataAsByteBuffer(ByteOrder.BIG_ENDIAN);
        byte[] data = new byte[totalElements];
        src.get(data);

        try {
            if (numChannels == 1) {
                return ByteBuffer.wrap(encodeGray(data, width, height));
            }
            if ("rgb".equals(getEncodedColorSpace())) {
                return ByteBuffer.wrap(encodeRgbNoConversion(data, width, height));
            }
            int[] luma = lumaSamplingFactors();
            return ByteBuffer.wrap(encodeYCbCr(data, width, height, luma[0], luma[1]));
        } catch (IOException ex) {
            throw new ZarrException("Error in encoding jpeg.", ex);
        }
    }

    /** Encode single-component grayscale by writing the raw raster (no color transform). */
    private byte[] encodeGray(byte[] data, int width, int height) throws IOException {
        DataBufferByte dataBuffer = new DataBufferByte(data, data.length);
        WritableRaster raster = Raster.createInterleavedRaster(
                dataBuffer, width, height, width, 1, new int[]{0}, null);
        ImageWriter writer = getJpegWriter();
        try {
            return writeJpeg(writer, new IIOImage(raster, null, null), jpegWriteParam(writer));
        } finally {
            writer.dispose();
        }
    }

    /**
     * Encode three components as YCbCr with the given luma sampling factors. The data is handed to
     * the writer as an sRGB {@link BufferedImage} so that the writer performs the RGB to YCbCr
     * conversion and writes a JFIF marker; the sampling factors are set on the stream metadata.
     */
    private byte[] encodeYCbCr(byte[] data, int width, int height, int hSampling, int vSampling)
            throws IOException {
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        for (int pixel = 0; pixel < width * height; pixel++) {
            int r = data[pixel * 3] & 0xff;
            int g = data[pixel * 3 + 1] & 0xff;
            int b = data[pixel * 3 + 2] & 0xff;
            image.setRGB(pixel % width, pixel / width, (r << 16) | (g << 8) | b);
        }
        ImageWriter writer = getJpegWriter();
        try {
            ImageWriteParam param = jpegWriteParam(writer);
            IIOMetadata metadata =
                    writer.getDefaultImageMetadata(new ImageTypeSpecifier(image), param);
            String format = metadata.getNativeMetadataFormatName();
            Node tree = metadata.getAsTree(format);
            setLumaSamplingFactors(tree, hSampling, vSampling);
            metadata.setFromTree(format, tree);
            return writeJpeg(writer, new IIOImage(image, null, metadata), param);
        } finally {
            writer.dispose();
        }
    }

    /**
     * Encode three components without any color transform: the raw raster is written as-is and an
     * APP14 Adobe marker with an "unknown" transform is added so that decoders do not apply an
     * inverse YCbCr transform. Chroma subsampling is not applicable, so 4:4:4 (1x1) is used.
     */
    private byte[] encodeRgbNoConversion(byte[] data, int width, int height) throws IOException {
        DataBufferByte dataBuffer = new DataBufferByte(data, data.length);
        WritableRaster raster = Raster.createInterleavedRaster(
                dataBuffer, width, height, width * 3, 3, new int[]{0, 1, 2}, null);
        ImageWriter writer = getJpegWriter();
        try {
            ImageWriteParam param = jpegWriteParam(writer);
            ImageTypeSpecifier its = ImageTypeSpecifier.createInterleaved(
                    ColorSpace.getInstance(ColorSpace.CS_sRGB), new int[]{0, 1, 2}, DataBuffer.TYPE_BYTE,
                    false, false);
            IIOMetadata metadata = writer.getDefaultImageMetadata(its, param);
            String format = metadata.getNativeMetadataFormatName();
            Node tree = metadata.getAsTree(format);
            setLumaSamplingFactors(tree, 1, 1);
            setAdobeTransform(tree, ADOBE_TRANSFORM_UNKNOWN);
            metadata.setFromTree(format, tree);
            return writeJpeg(writer, new IIOImage(raster, null, metadata), param);
        } finally {
            writer.dispose();
        }
    }

    private ImageWriteParam jpegWriteParam(ImageWriter writer) {
        ImageWriteParam param = writer.getDefaultWriteParam();
        param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
        param.setCompressionQuality(getQualityInternal() / 100f);
        return param;
    }

    private byte[] writeJpeg(ImageWriter writer, IIOImage image, ImageWriteParam param)
            throws IOException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
             ImageOutputStream imageOutputStream = ImageIO.createImageOutputStream(outputStream)) {
            writer.setOutput(imageOutputStream);
            writer.write(null, image, param);
            imageOutputStream.flush();
            return outputStream.toByteArray();
        }
    }

    @Override
    public Array decode(ByteBuffer chunkBytes) throws ZarrException {
        int numChannels = determineNumChannels();
        validateConfiguration(numChannels);
        try {
            byte[] jpegBytes = Utils.toArray(chunkBytes);
            byte[] data;
            if (numChannels == 1) {
                data = decodeRaster(jpegBytes);
            } else if (streamStoresYCbCr(jpegBytes)) {
                // A YCbCr (JFIF) stream needs the inverse color transform, which the high-level
                // reader applies based on the stream's markers.
                data = decodeYCbCr(jpegBytes);
            } else {
                // An "rgb" (APP14 transform=0) stream stores the components directly; read them raw.
                data = decodeRaster(jpegBytes);
            }
            return Array.factory(
                    DataType.UINT8.getMA2DataType(), arrayMetadata.chunkShape, ByteBuffer.wrap(data));
        } catch (IOException ex) {
            throw new ZarrException("Error in decoding jpeg.", ex);
        }
    }

    /** Decode the raw stored samples (no color transform) as an interleaved byte array. */
    private byte[] decodeRaster(byte[] jpegBytes) throws IOException {
        ImageReader reader = getJpegReader();
        try (ImageInputStream imageInputStream = ImageIO.createImageInputStream(
                new ByteArrayInputStream(jpegBytes))) {
            reader.setInput(imageInputStream);
            Raster raster = reader.readRaster(0, null);
            int width = raster.getWidth();
            int height = raster.getHeight();
            int numBands = raster.getNumBands();
            byte[] data = new byte[width * height * numBands];
            int[] pixel = new int[numBands];
            int index = 0;
            for (int y = 0; y < height; y++) {
                for (int x = 0; x < width; x++) {
                    raster.getPixel(x, y, pixel);
                    for (int b = 0; b < numBands; b++) {
                        data[index++] = (byte) pixel[b];
                    }
                }
            }
            return data;
        } finally {
            reader.dispose();
        }
    }

    /** Decode a YCbCr stream to interleaved RGB, letting the reader apply the inverse transform. */
    private byte[] decodeYCbCr(byte[] jpegBytes) throws IOException {
        BufferedImage image = ImageIO.read(new ByteArrayInputStream(jpegBytes));
        if (image == null) {
            throw new IOException("Could not decode jpeg image.");
        }
        int width = image.getWidth();
        int height = image.getHeight();
        byte[] data = new byte[width * height * 3];
        for (int pixel = 0; pixel < width * height; pixel++) {
            int rgb = image.getRGB(pixel % width, pixel / width);
            data[pixel * 3] = (byte) ((rgb >> 16) & 0xff);
            data[pixel * 3 + 1] = (byte) ((rgb >> 8) & 0xff);
            data[pixel * 3 + 2] = (byte) (rgb & 0xff);
        }
        return data;
    }

    /**
     * Inspect the JPEG markers to determine whether a 3-component stream stores its samples as
     * YCbCr (and therefore needs an inverse color transform on decode). Follows the usual
     * libjpeg/Adobe conventions: an APP14 Adobe marker's transform code wins; otherwise a JFIF
     * marker implies YCbCr; otherwise component ids of 'R','G','B' mean RGB and anything else
     * defaults to YCbCr.
     */
    private static boolean streamStoresYCbCr(byte[] jpegBytes) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(jpegBytes))) {
            if (in.readUnsignedShort() != 0xFFD8) { // SOI
                return false;
            }
            boolean jfif = false;
            Integer adobeTransform = null;
            int numComponents = 0;
            int[] componentIds = new int[4];
            while (in.available() > 0) {
                int marker = in.readUnsignedShort();
                if (marker == 0xFFDA) { // SOS: start of scan, header is complete
                    break;
                }
                int length = in.readUnsignedShort();
                byte[] segment = new byte[length - 2];
                in.readFully(segment);
                if (marker == 0xFFE0 && segment.length >= 4
                        && segment[0] == 'J' && segment[1] == 'F' && segment[2] == 'I' && segment[3] == 'F') {
                    jfif = true;
                } else if (marker == 0xFFEE && segment.length >= 12) { // APP14 Adobe
                    adobeTransform = segment[11] & 0xff;
                } else if (marker >= 0xFFC0 && marker <= 0xFFC3) { // SOF0..SOF3
                    numComponents = segment[5] & 0xff;
                    for (int c = 0; c < numComponents && c < componentIds.length; c++) {
                        componentIds[c] = segment[6 + c * 3] & 0xff;
                    }
                }
            }
            if (numComponents != 3) {
                return false;
            }
            if (adobeTransform != null) {
                return adobeTransform == ADOBE_TRANSFORM_YCBCR;
            }
            if (jfif) {
                return true;
            }
            boolean rgbIds =
                    componentIds[0] == 'R' && componentIds[1] == 'G' && componentIds[2] == 'B';
            return !rgbIds;
        }
    }

    /** Set the luma component sampling factors; chroma components are kept at 1x1. */
    private static void setLumaSamplingFactors(Node tree, int hSampling, int vSampling) {
        Node sof = findNode(tree, "sof");
        if (sof == null) {
            return;
        }
        NodeList children = sof.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (!"componentSpec".equals(child.getNodeName())) {
                continue;
            }
            NamedNodeMap attrs = child.getAttributes();
            boolean luma = "1".equals(attrs.getNamedItem("componentId").getNodeValue());
            attrs.getNamedItem("HsamplingFactor").setNodeValue(String.valueOf(luma ? hSampling : 1));
            attrs.getNamedItem("VsamplingFactor").setNodeValue(String.valueOf(luma ? vSampling : 1));
        }
    }

    /** Insert (or update) an APP14 Adobe marker with the given color transform code. */
    private static void setAdobeTransform(Node tree, int transform) {
        Node existing = findNode(tree, "app14Adobe");
        if (existing != null) {
            existing.getAttributes().getNamedItem("transform").setNodeValue(String.valueOf(transform));
            return;
        }
        Node markerSequence = findNode(tree, "markerSequence");
        if (markerSequence == null) {
            return;
        }
        IIOMetadataNode adobe = new IIOMetadataNode("app14Adobe");
        adobe.setAttribute("version", "100");
        adobe.setAttribute("flags0", "0");
        adobe.setAttribute("flags1", "0");
        adobe.setAttribute("transform", String.valueOf(transform));
        markerSequence.insertBefore(adobe, markerSequence.getFirstChild());
    }

    @Nullable
    private static Node findNode(Node node, String name) {
        if (name.equals(node.getNodeName())) {
            return node;
        }
        NodeList children = node.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node found = findNode(children.item(i), name);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static ImageWriter getJpegWriter() throws IOException {
        Iterator<ImageWriter> writers = ImageIO.getImageWritersByFormatName("jpeg");
        if (!writers.hasNext()) {
            throw new IOException("No jpeg ImageWriter available.");
        }
        return writers.next();
    }

    private static ImageReader getJpegReader() throws IOException {
        Iterator<ImageReader> readers = ImageIO.getImageReadersByFormatName("jpeg");
        if (!readers.hasNext()) {
            throw new IOException("No jpeg ImageReader available.");
        }
        return readers.next();
    }

    @Override
    public long computeEncodedSize(long inputByteLength,
                                   ArrayMetadata.CoreArrayMetadata arrayMetadata) throws ZarrException {
        throw new ZarrException("Not implemented for Jpeg codec.");
    }

    public static final class Configuration {

        static final int DEFAULT_QUALITY = 90;
        /** Default luma sampling factors when subsampling is unset (the 4:2:0 scheme). */
        static final int[] DEFAULT_LUMA_SAMPLING = {2, 2};

        public final int quality;
        @Nullable
        @JsonProperty("encoded_color_space")
        public final String encodedColorSpace;
        /**
         * Per-component JPEG sampling factors, one {@code [horizontal, vertical]} pair per component
         * (e.g. {@code [[2, 2], [1, 1], [1, 1]]} for the common 4:2:0 scheme), or {@code null} for the
         * default. Chroma components must be {@code [1, 1]}.
         */
        @Nullable
        public final int[][] subsampling;

        public Configuration(int quality) throws ZarrException {
            this(quality, null, null);
        }

        @JsonCreator
        public Configuration(
                @JsonProperty(value = "quality", defaultValue = "90") int quality,
                @Nullable @JsonProperty("encoded_color_space") String encodedColorSpace,
                @Nullable @JsonProperty("subsampling") int[][] subsampling) throws ZarrException {
            if (quality < 0 || quality > 100) {
                throw new ZarrException("'quality' needs to be between 0 and 100.");
            }
            if (encodedColorSpace != null
                    && !"ycbcr".equals(encodedColorSpace) && !"rgb".equals(encodedColorSpace)) {
                throw new ZarrException(
                        "'encoded_color_space' must be \"ycbcr\" or \"rgb\", got \"" + encodedColorSpace + "\".");
            }
            if (subsampling != null) {
                validateSubsampling(subsampling);
                if ("rgb".equals(encodedColorSpace) && !isNoSubsampling(subsampling)) {
                    throw new ZarrException(
                            "'subsampling' must be [[1, 1], [1, 1], [1, 1]] (or omitted) with "
                                    + "encoded_color_space \"rgb\", since those components are independent and "
                                    + "must not be subsampled.");
                }
            }
            this.quality = quality;
            this.encodedColorSpace = encodedColorSpace;
            this.subsampling = subsampling;
        }

        /**
         * Validate the structural constraints on the sampling factors that do not depend on the
         * channel count: each entry is a {@code [horizontal, vertical]} pair with factors in 1..4,
         * the chroma components (all but the first) are {@code [1, 1]}, and each luma factor is at
         * least the corresponding chroma factor.
         */
        private static void validateSubsampling(int[][] subsampling) throws ZarrException {
            for (int[] factors : subsampling) {
                if (factors.length != 2) {
                    throw new ZarrException(
                            "each 'subsampling' entry must be a [horizontal, vertical] pair.");
                }
                for (int factor : factors) {
                    if (factor < 1 || factor > 4) {
                        throw new ZarrException("'subsampling' factors must be between 1 and 4.");
                    }
                }
            }
            for (int i = 1; i < subsampling.length; i++) {
                if (subsampling[i][0] != 1 || subsampling[i][1] != 1) {
                    throw new ZarrException("the chroma components' 'subsampling' factor must be [1, 1].");
                }
            }
            if (subsampling.length > 1
                    && (subsampling[0][0] < 1 || subsampling[0][1] < 1)) {
                throw new ZarrException(
                        "each luma 'subsampling' factor must be >= the corresponding chroma factor.");
            }
        }

        private static boolean isNoSubsampling(int[][] subsampling) {
            for (int[] factors : subsampling) {
                if (factors.length != 2 || factors[0] != 1 || factors[1] != 1) {
                    return false;
                }
            }
            return true;
        }
    }
}
