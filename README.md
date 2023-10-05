## Early preview of `zarr-java`

This repository contains an early preview of a Java implementation of the Zarr specification. 
It is intended for collecting feedback from the community and not for use. 

# zarr-java
Java implementation of the Zarr Specification

## Usage
```java
import com.scalableminds.zarrjava.store.FilesystemStore;
import com.scalableminds.zarrjava.store.HttpStore;
import com.scalableminds.zarrjava.v3.Array;
import com.scalableminds.zarrjava.v3.DataType;
import com.scalableminds.zarrjava.v3.Group;

Group hierarchy = Group.open(
    new HttpStore("https://static.webknossos.org/data/zarr_v3")
        .resolve("l4_sample")
);
Array array = hierarchy.get("color").get("1");
ucar.ma2.Array outArray = array.read(
    new long[]{0, 3073, 3073, 513}, 
    new int[]{1, 64, 64, 64}
);

Array array = Array.create(
    new FilesystemStore("/path/to/zarr").resolve("array"),
    Array.metadataBuilder()
        .withShape(1, 4096, 4096, 1536)
        .withDataType(DataType.UINT32)
        .withChunkShape(1, 1024, 1024, 1024)
        .withFillValue(0)
        .withCodecs(c -> c.withSharding(new int[]{1, 32, 32, 32}, c1 -> c1.withBlosc()))
        .build();
);
array.write(
    new long[]{0, 0, 0, 0}, 
    ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, new int[]{1, 1024, 1024, 1024})
);
```