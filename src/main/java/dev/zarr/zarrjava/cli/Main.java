package dev.zarr.zarrjava.cli;

import dev.zarr.zarrjava.core.Array;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

@Command(name = "zarr-java-cli", mixinStandardHelpOptions = true, version = "1.0", description = "CLI wrapper for zarr-java conformance tests.")
public class Main implements Callable<Integer> {

    @Option(names = { "--array_path" }, description = "Path to the Zarr array", required = true)
    private String arrayPath;

    @Override
    public Integer call() throws Exception {
        try {
            Path path = Paths.get(arrayPath);
            // Attempt to open the array. This should throw if the array is invalid or
            // cannot be opened.
            Array array = Array.open(path);

            // Read the entire array
            ucar.ma2.Array data = array.read();

            // Print the array values using ucar.ma2.Array's string representation.
            System.out.println(data.toString());

            return 0;
        } catch (Exception e) {
            System.err.println("Failed to open array at " + arrayPath);
            e.printStackTrace();
            return 1;
        }
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }
}
