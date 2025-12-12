package dev.zarr.zarrjava.utils;

import javax.annotation.Nullable;
import java.io.IOException;

public class ZipUtils {

    // adopted from https://stackoverflow.com/a/9918966
    @Nullable
    public static String getZipCommentFromBuffer(byte[] bufArray) throws IOException {
        // End of Central Directory (EOCD) record magic number
        byte[]  EOCD = {0x50, 0x4b, 0x05, 0x06};
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

}
