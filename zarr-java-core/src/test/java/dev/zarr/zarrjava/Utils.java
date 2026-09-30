package dev.zarr.zarrjava;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class Utils {

    public static void zipFile(Path sourceDir, Path targetDir) throws IOException {
        FileOutputStream fos = new FileOutputStream(targetDir.toFile());
        ZipOutputStream zipOut = new ZipOutputStream(fos);

        File fileToZip = new File(sourceDir.toUri());

        zipFile(fileToZip, "", zipOut);
        zipOut.close();
        fos.close();
    }

    static void zipFile(File fileToZip, String fileName, ZipOutputStream zipOut) throws IOException {
        if (fileToZip.isDirectory()) {
            if (fileName.endsWith("/")) {
                zipOut.putNextEntry(new ZipEntry(fileName));
                zipOut.closeEntry();
            } else {
                zipOut.putNextEntry(new ZipEntry(fileName + "/"));
                zipOut.closeEntry();
            }
            File[] children = fileToZip.listFiles();
            for (File childFile : children) {
                zipFile(childFile, fileName + "/" + childFile.getName(), zipOut);
            }
            return;
        }
        FileInputStream fis = new FileInputStream(fileToZip);
        ZipEntry zipEntry = new ZipEntry(fileName);
        zipOut.putNextEntry(zipEntry);
        byte[] bytes = new byte[1024];
        int length;
        while ((length = fis.read(bytes)) >= 0) {
            zipOut.write(bytes, 0, length);
        }
        fis.close();
    }

    /**
     * Unzip sourceZip into targetDir.
     * Protects against Zip Slip by ensuring extracted paths remain inside targetDir.
     */
    public static void unzipFile(Path sourceZip, Path targetDir) throws IOException {
        Files.createDirectories(targetDir);
        try (FileInputStream fis = new FileInputStream(sourceZip.toFile());
             ZipInputStream zis = new ZipInputStream(fis)) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                Path outPath = targetDir.resolve(entry.getName()).normalize();
                Path targetDirNorm = targetDir.normalize();
                if (!outPath.startsWith(targetDirNorm)) {
                    throw new IOException("Zip entry is outside of the target dir: " + entry.getName());
                }
                if (entry.isDirectory() || entry.getName().endsWith("/")) {
                    Files.createDirectories(outPath);
                } else {
                    Files.createDirectories(outPath.getParent());
                    try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(outPath.toFile()))) {
                        byte[] buffer = new byte[1024];
                        int len;
                        while ((len = zis.read(buffer)) > 0) {
                            bos.write(buffer, 0, len);
                        }
                    }
                }
                zis.closeEntry();
            }
        }
    }

}
