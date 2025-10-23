# Early preview of `zarr-java`

This repository contains an early preview of a Java implementation of the Zarr specification. 
It is intended for collecting feedback from the community and not for use. The API is subject to changes.

Refer to [JZarr](https://github.com/zarr-developers/jzarr) for a stable implementation of Zarr version 2.

## Usage
```java
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.HttpStore;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.Group;

Group hierarchy = Group.open(
    new HttpStore("https://static.webknossos.org/data/zarr_v3")
        .resolve("l4_sample")
);
Group color = (Group) hierarchy.get("color");
Array array = (Array) color.get("1");
ucar.ma2.Array outArray = array.read(
    new long[]{0, 3073, 3073, 513}, // offset
    new int[]{1, 64, 64, 64} // shape
);

Array array = Array.create(
    new FilesystemStore("/path/to/zarr").resolve("array"),
    Array.metadataBuilder()
        .withShape(1, 4096, 4096, 1536)
        .withDataType(DataType.UINT32)
        .withChunkShape(1, 1024, 1024, 1024)
        .withFillValue(0)
        .withCodecs(c -> c.withSharding(new int[]{1, 32, 32, 32}, c1 -> c1.withBlosc()))
        .build()
);
ucar.ma2.Array data = ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, new int[]{1, 1024, 1024, 1024});
array.write(
    new long[]{0, 0, 0, 0}, // offset
    data
);
```
## Development Start-Guide

### Run Tests Locally
To be able to run the tests locally, make sure to have `python3.11` and `uv` installed. 

Furthermore, you will need the `l4_sample` test data:

`curl https://static.webknossos.org/data/zarr_v3/l4_sample.zip -o testdata/l4_sample.zip
&& cd testdata
&& unzip l4_sample.zip
`