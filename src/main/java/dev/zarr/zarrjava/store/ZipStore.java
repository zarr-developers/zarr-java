package dev.zarr.zarrjava.store;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class ZipStore implements Store, Store.ListableStore {
    public final StoreHandle underlyingStore;

    public ZipStore(@Nonnull StoreHandle underlyingStore) {
        this.underlyingStore = underlyingStore;
    }

    // adopted from https://stackoverflow.com/a/9918966
    @Nullable
    public static String getZipCommentFromBuffer(byte[] bufArray) throws IOException {
        // End of Central Directory (EOCD) record magic number
        byte[] EOCD = {0x50, 0x4b, 0x05, 0x06};
        int buffLen = bufArray.length;
        // Check the buffer from the end
        search:
        for (int i = buffLen - EOCD.length - 22; i >= 0; i--) {
            for (int k = 0; k < EOCD.length; k++) {
                if (bufArray[i + k] != EOCD[k]) {
                    continue search;
                }
            }
            // End of Central Directory found!
            int commentLen = bufArray[i + 20] + bufArray[i + 21] * 256;
            int realLen = buffLen - i - 22;
            if (commentLen != realLen) {
                throw new IOException("ZIP comment size mismatch: "
                        + "directory says len is " + commentLen
                        + ", but file ends after " + realLen + " bytes!");
            }
            return new String(bufArray, i + 22, commentLen);
        }
        return null;
    }

    public String getArchiveComment() throws IOException {
        // Attempt to read from the end of the file to find the EOCD record.
        // We try a small chunk first (1KB) which covers most short comments (or no comment),
        // then the maximum possible EOCD size (approx 65KB).
        long fileSize = underlyingStore.getSize();
        if (fileSize < 22) {
            return null;
        }
        int[] readSizes = {1024, 65535 + 22};

        for (int size : readSizes) {
            ByteBuffer buffer;

            if (fileSize < size) {
                buffer = underlyingStore.read();
            } else {
                buffer = underlyingStore.read(fileSize - size);
            }

            if (buffer == null) {
                return null;
            }

            byte[] bufArray;
            if (buffer.hasArray()) {
                bufArray = buffer.array();
            } else {
                bufArray = new byte[buffer.remaining()];
                buffer.duplicate().get(bufArray);
            }

            String comment = getZipCommentFromBuffer(bufArray);
            if (comment != null) {
                return comment;
            }
            if (fileSize < size) {
                break;
            }
        }
        return null;
    }
}