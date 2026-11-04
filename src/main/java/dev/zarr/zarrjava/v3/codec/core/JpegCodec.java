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
 * <p>The required {@code encoded_color_space} selects how the samples are stored and fixes the
 * component count: {@code grayscale} (1 component), {@code ycbcr} (the JFIF color space, enabling
 * chroma subsampling) or {@code rgb} (the three components stored without conversion, with an
 * APP14 Adobe marker with an "unknown" transform so decoders do not apply an inverse YCbCr
 * transform). The optional {@code decoded_color_space} is the color space of the chunk data; it
 * defaults to {@code grayscale} for grayscale and {@code rgb} otherwise. On decode the stored
 * color space is determined by the stream's markers, and converted to {@code decoded_color_space}.
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

    public static final String GRAYSCALE = "grayscale";
    public static final String RGB = "rgb";
    public static final String YCBCR = "ycbcr";

    @JsonIgnore
    public final String name = "jpeg";
    public final Configuration configuration;

    @JsonCreator
    public JpegCodec(
            @JsonProperty(value = "configuration") Configuration configuration) throws ZarrException {
        if (configuration == null) {
            throw new ZarrException("The jpeg codec requires a 'configuration'.");
        }
        this.configuration = configuration;
    }

    public JpegCodec() throws ZarrException {
        this(Configuration.DEFAULT_QUALITY);
    }

    public JpegCodec(int quality) throws ZarrException {
        this(new Configuration(quality));
    }

    public JpegCodec(int quality, String encodedColorSpace, @Nullable int[][] subsampling)
            throws ZarrException {
        this(new Configuration(quality, encodedColorSpace, subsampling));
    }

    public JpegCodec(int quality, String encodedColorSpace, @Nullable String decodedColorSpace,
                     @Nullable int[][] subsampling) throws ZarrException {
        this(new Configuration(quality, encodedColorSpace, decodedColorSpace, subsampling));
    }

    /**
     * Check that the chunk is {@code uint8} and its shape matches the configured component count.
     * {@code (H, W)} and {@code (H, W, 1)} are 1-component; {@code (H, W, 3)} is 3-component;
     * everything else (1D, an unsupported channel count, or 4D and higher) is an error.
     */
    private void validateChunk() throws ZarrException {
        if (arrayMetadata.dataType != DataType.UINT8) {
            throw new ZarrException(
                    "The jpeg codec only supports the uint8 data type, got " + arrayMetadata.dataType + ".");
        }
        int[] shape = arrayMetadata.chunkShape;
        int numComponents;
        if (shape.length == 2) {
            numComponents = 1;
        } else if (shape.length == 3 && (shape[2] == 1 || shape[2] == 3)) {
            numComponents = shape[2];
        } else {
            throw new ZarrException(
                    "The jpeg codec only supports chunk shapes (H, W), (H, W, 1) or (H, W, 3), got "
                            + Arrays.toString(shape)
                            + ". Reshape or transpose the data so the channel axis (of extent 1 or 3) "
                            + "is innermost before the jpeg codec.");
        }
        if (numComponents != configuration.numComponents()) {
            throw new ZarrException(
                    "The chunk shape " + Arrays.toString(shape) + " has " + numComponents
                            + " component(s), but encoded_color_space \"" + configuration.encodedColorSpace
                            + "\" requires " + configuration.numComponents() + ".");
        }
        if (shape[0] > MAX_JPEG_DIMENSION || shape[1] > MAX_JPEG_DIMENSION) {
            throw new ZarrException(
                    "JPEG image dimensions must not exceed " + MAX_JPEG_DIMENSION + ", got height="
                            + shape[0] + ", width=" + shape[1] + ".");
        }
    }

    @Override
    public ByteBuffer encode(Array chunkArray) throws ZarrException {
        validateChunk();

        int[] shape = arrayMetadata.chunkShape;
        int height = shape[0];
        int width = shape[1];
        int totalElements = (int) chunkArray.getSize();
        ByteBuffer src = chunkArray.getDataAsByteBuffer(ByteOrder.BIG_ENDIAN);
        byte[] data = new byte[totalElements];
        src.get(data);

        int[] luma = configuration.subsampling[0];
        try {
            if (GRAYSCALE.equals(configuration.encodedColorSpace)) {
                return ByteBuffer.wrap(encodeGray(data, width, height));
            }
            if (RGB.equals(configuration.encodedColorSpace)) {
                return ByteBuffer.wrap(encodeRgbNoConversion(data, width, height));
            }
            if (YCBCR.equals(configuration.decodedColorSpace)) {
                return ByteBuffer.wrap(encodeYCbCrNoConversion(data, width, height, luma[0], luma[1]));
            }
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
     * inverse YCbCr transform. The JFIF marker is dropped, since libjpeg treats any 3-component
     * JFIF stream as YCbCr regardless of the Adobe marker. Chroma subsampling is not applicable, so
     * 4:4:4 (1x1) is used.
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
            removeNode(tree, "app0JFIF");
            setAdobeTransform(tree, ADOBE_TRANSFORM_UNKNOWN);
            metadata.setFromTree(format, tree);
            return writeJpeg(writer, new IIOImage(raster, null, metadata), param);
        } finally {
            writer.dispose();
        }
    }

    /**
     * Encode three components that are already YCbCr: the raw raster is written as-is (the writer
     * does not color-convert rasters) together with a JFIF marker, so the stream records YCbCr.
     */
    private byte[] encodeYCbCrNoConversion(byte[] data, int width, int height, int hSampling,
                                           int vSampling) throws IOException {
        DataBufferByte dataBuffer = new DataBufferByte(data, data.length);
        WritableRaster raster = Raster.createInterleavedRaster(
                dataBuffer, width, height, width * 3, 3, new int[]{0, 1, 2}, null);
        ImageWriter writer = getJpegWriter();
        try {
            ImageWriteParam param = jpegWriteParam(writer);
            IIOMetadata metadata = writer.getDefaultImageMetadata(
                    ImageTypeSpecifier.createFromBufferedImageType(BufferedImage.TYPE_3BYTE_BGR), param);
            String format = metadata.getNativeMetadataFormatName();
            Node tree = metadata.getAsTree(format);
            setLumaSamplingFactors(tree, hSampling, vSampling);
            metadata.setFromTree(format, tree);
            return writeJpeg(writer, new IIOImage(raster, null, metadata), param);
        } finally {
            writer.dispose();
        }
    }

    private ImageWriteParam jpegWriteParam(ImageWriter writer) {
        ImageWriteParam param = writer.getDefaultWriteParam();
        param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
        param.setCompressionQuality(configuration.quality / 100f);
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
        validateChunk();
        try {
            byte[] jpegBytes = Utils.toArray(chunkBytes);
            // The stored color space is taken from the stream's markers, not from
            // encoded_color_space; only the conversion to decoded_color_space is configured.
            byte[] data;
            if (GRAYSCALE.equals(configuration.decodedColorSpace)) {
                data = decodeRaster(jpegBytes);
            } else if (!streamStoresYCbCr(jpegBytes)) {
                if (YCBCR.equals(configuration.decodedColorSpace)) {
                    throw new ZarrException(
                            "Cannot decode a jpeg stream that does not store YCbCr to decoded_color_space "
                                    + "\"ycbcr\".");
                }
                // An "rgb" (APP14 transform=0) stream stores the components directly; read them raw.
                data = decodeRaster(jpegBytes);
            } else if (YCBCR.equals(configuration.decodedColorSpace)) {
                // Raster reads skip the inverse color transform and return the stored YCbCr samples.
                data = decodeRaster(jpegBytes);
            } else {
                // A YCbCr (JFIF) stream needs the inverse color transform, which the high-level
                // reader applies based on the stream's markers.
                data = decodeYCbCr(jpegBytes);
            }
            int expectedLength = Arrays.stream(arrayMetadata.chunkShape).reduce(1, (a, b) -> a * b);
            if (data.length != expectedLength) {
                throw new ZarrException(
                        "Decoded jpeg has " + data.length + " samples, but the chunk shape "
                                + Arrays.toString(arrayMetadata.chunkShape) + " requires " + expectedLength
                                + ".");
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
     * YCbCr (and therefore needs an inverse color transform on decode). Follows libjpeg's
     * precedence so all implementations agree: a JFIF marker implies YCbCr; otherwise an APP14
     * Adobe marker's transform code decides; otherwise component ids of 'R','G','B' mean RGB and
     * anything else defaults to YCbCr.
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
            if (jfif) {
                return true;
            }
            if (adobeTransform != null) {
                return adobeTransform == ADOBE_TRANSFORM_YCBCR;
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

    private static void removeNode(Node tree, String name) {
        Node node = findNode(tree, name);
        if (node != null) {
            node.getParentNode().removeChild(node);
        }
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

        /** Quality used when building a configuration without an explicit quality, per the spec. */
        static final int DEFAULT_QUALITY = 95;

        public final int quality;
        /** Color space of the samples as stored in the JPEG stream; fixes the component count. */
        @JsonProperty("encoded_color_space")
        public final String encodedColorSpace;
        /**
         * Color space of the chunk data. Always resolved; it is only written to the metadata when it
         * differs from the default for {@link #encodedColorSpace}.
         */
        @JsonIgnore
        public final String decodedColorSpace;
        /**
         * Per-component JPEG sampling factors, one {@code [horizontal, vertical]} pair per component
         * (e.g. {@code [[2, 2], [1, 1], [1, 1]]} for the common 4:2:0 scheme, or {@code [[1, 1]]}
         * for grayscale). Chroma components must be {@code [1, 1]}. Always resolved; it is only
         * required for, and only written to the metadata with, {@code ycbcr}, since {@code grayscale}
         * and {@code rgb} are never subsampled.
         */
        @JsonIgnore
        public final int[][] subsampling;

        /** A grayscale (1-component) configuration with the given quality. */
        public Configuration(int quality) throws ZarrException {
            this(quality, GRAYSCALE, null, null);
        }

        public Configuration(int quality, String encodedColorSpace, @Nullable int[][] subsampling)
                throws ZarrException {
            this(quality, encodedColorSpace, null, subsampling);
        }

        @JsonCreator
        public Configuration(
                @JsonProperty("quality") Integer quality,
                @JsonProperty("encoded_color_space") String encodedColorSpace,
                @Nullable @JsonProperty("decoded_color_space") String decodedColorSpace,
                @Nullable @JsonProperty("subsampling") int[][] subsampling) throws ZarrException {
            if (quality == null) {
                throw new ZarrException("'quality' is required for the jpeg codec.");
            }
            if (encodedColorSpace == null) {
                throw new ZarrException("'encoded_color_space' is required for the jpeg codec.");
            }
            if (quality < 1 || quality > 100) {
                throw new ZarrException("'quality' needs to be between 1 and 100.");
            }
            if (!isColorSpace(encodedColorSpace)) {
                throw new ZarrException("'encoded_color_space' must be \"grayscale\", \"ycbcr\" or "
                        + "\"rgb\", got \"" + encodedColorSpace + "\".");
            }
            if (decodedColorSpace == null) {
                decodedColorSpace = defaultDecodedColorSpace(encodedColorSpace);
            } else if (!isColorSpace(decodedColorSpace)) {
                throw new ZarrException("'decoded_color_space' must be \"grayscale\", \"ycbcr\" or "
                        + "\"rgb\", got \"" + decodedColorSpace + "\".");
            }
            if (!isSupportedConversion(decodedColorSpace, encodedColorSpace)) {
                throw new ZarrException("decoded_color_space \"" + decodedColorSpace
                        + "\" cannot be combined with encoded_color_space \"" + encodedColorSpace + "\".");
            }
            int numComponents = GRAYSCALE.equals(encodedColorSpace) ? 1 : 3;
            if (subsampling == null) {
                if (YCBCR.equals(encodedColorSpace)) {
                    throw new ZarrException(
                            "'subsampling' is required for the jpeg codec with encoded_color_space \"ycbcr\".");
                }
                subsampling = noSubsampling(numComponents);
            }
            validateSubsampling(subsampling, numComponents);
            if (!YCBCR.equals(encodedColorSpace) && !isNoSubsampling(subsampling)) {
                throw new ZarrException("'subsampling' must not subsample with encoded_color_space \""
                        + encodedColorSpace + "\"; only \"ycbcr\" supports chroma subsampling.");
            }
            this.quality = quality;
            this.encodedColorSpace = encodedColorSpace;
            this.decodedColorSpace = decodedColorSpace;
            this.subsampling = subsampling;
        }

        /** {@code decoded_color_space} as written to the metadata: omitted when it is the default. */
        @Nullable
        @JsonProperty("decoded_color_space")
        String serializedDecodedColorSpace() {
            return decodedColorSpace.equals(defaultDecodedColorSpace(encodedColorSpace))
                    ? null : decodedColorSpace;
        }

        /** {@code subsampling} as written to the metadata: omitted unless it is {@code ycbcr}. */
        @Nullable
        @JsonProperty("subsampling")
        int[][] serializedSubsampling() {
            return YCBCR.equals(encodedColorSpace) ? subsampling : null;
        }

        /** The number of components (1 or 3) fixed by {@link #encodedColorSpace}. */
        public int numComponents() {
            return subsampling.length;
        }

        private static boolean isColorSpace(String colorSpace) {
            return GRAYSCALE.equals(colorSpace) || RGB.equals(colorSpace) || YCBCR.equals(colorSpace);
        }

        private static String defaultDecodedColorSpace(String encodedColorSpace) {
            return GRAYSCALE.equals(encodedColorSpace) ? GRAYSCALE : RGB;
        }

        /** The decoded/encoded color space combinations allowed by the spec. */
        private static boolean isSupportedConversion(String decoded, String encoded) {
            switch (decoded) {
                case GRAYSCALE:
                    return GRAYSCALE.equals(encoded);
                case RGB:
                    return RGB.equals(encoded) || YCBCR.equals(encoded);
                default:
                    return YCBCR.equals(encoded);
            }
        }

        /**
         * Validate the sampling factors: one {@code [horizontal, vertical]} pair per component with
         * factors in 1..4, and the chroma components (all but the first) are {@code [1, 1]}.
         */
        private static void validateSubsampling(int[][] subsampling, int numComponents)
                throws ZarrException {
            if (subsampling.length != numComponents) {
                throw new ZarrException("'subsampling' must have one entry per component ("
                        + numComponents + "), got " + subsampling.length + ".");
            }
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
        }

        private static int[][] noSubsampling(int numComponents) {
            int[][] subsampling = new int[numComponents][];
            for (int i = 0; i < numComponents; i++) {
                subsampling[i] = new int[]{1, 1};
            }
            return subsampling;
        }

        static boolean isNoSubsampling(int[][] subsampling) {
            for (int[] factors : subsampling) {
                if (factors[0] != 1 || factors[1] != 1) {
                    return false;
                }
            }
            return true;
        }
    }
}
