package dev.zarr.zarrjava;

import org.junit.jupiter.api.BeforeAll;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;

public class ZarrTest {

    public static final Path TESTDATA = Paths.get("testdata");
    public static final Path TESTOUTPUT = Paths.get("testoutput");

    @BeforeAll
    public static void clearTestoutputFolder() throws IOException {
        if (Files.exists(TESTOUTPUT)) {
            try (Stream<Path> walk = Files.walk(TESTOUTPUT)) {
                walk.sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(file -> {
                        if (!file.delete()) {
                            throw new RuntimeException("Failed to delete file: " + file.getAbsolutePath());
                        }
                    });
            }
        }
        Files.createDirectory(TESTOUTPUT);
    }
}
