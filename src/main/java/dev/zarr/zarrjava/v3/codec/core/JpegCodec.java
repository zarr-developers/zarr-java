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
import ucar.ma2.Array;

import javax.annotation.Nullable;
import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageOutputStream;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;

/**
 * Encodes a {@code uint8} chunk as a 1- or 3-channel baseline JPEG image, compatible with
 * neuroglancer's {@code precomputed} "jpeg" chunk encoding.
 *
 * <p>The innermost (last) chunk axis is interpreted as the interleaved channel axis when its
 * extent is 3 (RGB); otherwise the chunk is treated as single-channel (grayscale). The remaining
 * axes, flattened in C-order (last-axis-fastest), form the pixel raster. This matches
 * neuroglancer's requirement that the pixels read row-by-row equal the chunk flattened in
 * {@code [x, y, z]} Fortran order with channels as interleaved image components, provided the
 * chunk is laid out as C-order {@code [z, y, x, channel]} (grayscale = {@code [z, y, x]}). Chunks
 * whose channel axis sits elsewhere can be reordered with a {@code transpose} codec placed before
 * this codec in the pipeline.
 *
 * <p>JPEG is lossy, so this codec must not be used where exact values matter (e.g. segmentation).
 */
public class JpegCodec extends ArrayBytesCodec implements Codec {

    /** JPEG images may not exceed this size along either dimension. */
    private static final int MAX_JPEG_DIMENSION = 65535;

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

    private int quality() {
        return configuration == null ? Configuration.DEFAULT_QUALITY : configuration.quality;
    }

    private int numChannels() {
        int[] shape = arrayMetadata.chunkShape;
        int lastAxis = shape[shape.length - 1];
        return lastAxis == 3 ? 3 : 1;
    }

    /**
     * Factor {@code numPixels} into a {@code width x height} such that {@code width * height ==
     * numPixels} and both are within the JPEG dimension limit. The exact factorization is
     * irrelevant for round-tripping (decode reads all pixels regardless of shape).
     */
    private static int[] factorWidthHeight(int numPixels) throws ZarrException {
        if (numPixels <= MAX_JPEG_DIMENSION) {
            return new int[]{numPixels, 1};
        }
        for (int height = 2; height <= numPixels; height++) {
            if (numPixels % height == 0) {
                int width = numPixels / height;
                if (width <= MAX_JPEG_DIMENSION && height <= MAX_JPEG_DIMENSION) {
                    return new int[]{width, height};
                }
            }
        }
        throw new ZarrException(
                "Chunk with " + numPixels + " pixels cannot be represented as a JPEG image "
                        + "(no width x height factorization fits within " + MAX_JPEG_DIMENSION + ").");
    }

    @Override
    public ByteBuffer encode(Array chunkArray) throws ZarrException {
        if (arrayMetadata.dataType != DataType.UINT8) {
            throw new ZarrException(
                    "The jpeg codec only supports the uint8 data type, got " + arrayMetadata.dataType + ".");
        }
        int numChannels = numChannels();
        int totalElements = (int) chunkArray.getSize();
        int numPixels = totalElements / numChannels;
        int[] wh = factorWidthHeight(numPixels);
        int width = wh[0];
        int height = wh[1];

        ByteBuffer src = chunkArray.getDataAsByteBuffer(ByteOrder.BIG_ENDIAN);
        byte[] data = new byte[totalElements];
        src.get(data);

        try {
            if (numChannels == 1) {
                return ByteBuffer.wrap(encodeGray(data, width, height));
            }
            return ByteBuffer.wrap(encodeRgb(data, width, height));
        } catch (IOException ex) {
            throw new ZarrException("Error in encoding jpeg.", ex);
        }
    }

    private byte[] encodeGray(byte[] data, int width, int height) throws IOException {
        DataBufferByte dataBuffer = new DataBufferByte(data, data.length);
        WritableRaster raster = Raster.createInterleavedRaster(
                dataBuffer, width, height, width, 1, new int[]{0}, null);
        return writeJpeg(new IIOImage(raster, null, null));
    }

    private byte[] encodeRgb(byte[] data, int width, int height) throws IOException {
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        for (int pixel = 0; pixel < width * height; pixel++) {
            int r = data[pixel * 3] & 0xff;
            int g = data[pixel * 3 + 1] & 0xff;
            int b = data[pixel * 3 + 2] & 0xff;
            image.setRGB(pixel % width, pixel / width, (r << 16) | (g << 8) | b);
        }
        return writeJpeg(new IIOImage(image, null, null));
    }

    private byte[] writeJpeg(IIOImage image) throws IOException {
        ImageWriter writer = getJpegWriter();
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
             ImageOutputStream imageOutputStream = ImageIO.createImageOutputStream(outputStream)) {
            writer.setOutput(imageOutputStream);
            ImageWriteParam param = writer.getDefaultWriteParam();
            param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
            param.setCompressionQuality(quality() / 100f);
            writer.write(null, image, param);
            imageOutputStream.flush();
            return outputStream.toByteArray();
        } finally {
            writer.dispose();
        }
    }

    @Override
    public Array decode(ByteBuffer chunkBytes) throws ZarrException {
        int numChannels = numChannels();
        try {
            byte[] data = numChannels == 1
                    ? decodeGray(Utils.toArray(chunkBytes))
                    : decodeRgb(Utils.toArray(chunkBytes));
            return Array.factory(
                    DataType.UINT8.getMA2DataType(), arrayMetadata.chunkShape, ByteBuffer.wrap(data));
        } catch (IOException ex) {
            throw new ZarrException("Error in decoding jpeg.", ex);
        }
    }

    private byte[] decodeGray(byte[] jpegBytes) throws IOException {
        ImageReader reader = getJpegReader();
        try (ImageInputStream imageInputStream = ImageIO.createImageInputStream(
                new ByteArrayInputStream(jpegBytes))) {
            reader.setInput(imageInputStream);
            // Read the raw raster to avoid any color-space conversion for single-channel data.
            Raster raster = reader.readRaster(0, null);
            int numPixels = raster.getWidth() * raster.getHeight();
            int[] samples = raster.getSamples(0, 0, raster.getWidth(), raster.getHeight(), 0,
                    new int[numPixels]);
            byte[] data = new byte[numPixels];
            for (int i = 0; i < numPixels; i++) {
                data[i] = (byte) samples[i];
            }
            return data;
        } finally {
            reader.dispose();
        }
    }

    private byte[] decodeRgb(byte[] jpegBytes) throws IOException {
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

        public final int quality;

        @JsonCreator
        public Configuration(
                @JsonProperty(value = "quality", defaultValue = "90") int quality) throws ZarrException {
            if (quality < 0 || quality > 100) {
                throw new ZarrException("'quality' needs to be between 0 and 100.");
            }
            this.quality = quality;
        }
    }
}
