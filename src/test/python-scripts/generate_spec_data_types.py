"""Regenerate the external Zarr v3 data-type conformance list.

The list this emits is deliberately NOT derived from zarr-java's own
``DataType`` enum. Deriving it from our enum would make the conformance test a
tautology: it could only ever assert that what we implemented is implemented,
so a data type we never added stays invisible. Instead we take the list from a
reference implementation (zarr-python), which tells us what will realistically
show up in stores that other tools have written.

Usage (from the repository root):

    uv run src/test/python-scripts/generate_spec_data_types.py

Writes ``src/test/resources/spec-data-types-v3.json``. The output is committed
so the Java conformance test runs offline, with no Python involved. Re-run this
when zarr-python is upgraded -- the nightly ``interop`` job is the natural place
to notice that it has drifted.
"""

import json
import sys
from pathlib import Path

import zarr
from zarr.core.dtype import data_type_registry

# Parameterized data types cannot be instantiated with no arguments. These are
# representative instances -- the point is to capture the JSON *shape* that
# appears in a real zarr.json, not to enumerate every possible parameterization.
PARAMETERIZED_ARGS = {
    "fixed_length_utf32": {"length": 4},
    "raw_bytes": {"length": 4},
    "null_terminated_bytes": {"length": 4},
    "numpy.datetime64": {"unit": "s", "scale_factor": 1},
    "numpy.timedelta64": {"unit": "s", "scale_factor": 1},
    "structured": {"fields": (("a", data_type_registry.contents["int32"]()),
                              ("b", data_type_registry.contents["float64"]()))},
}

OUT_PATH = Path("src/test/resources/spec-data-types-v3.json")


def instantiate(name, cls):
    """Build a representative instance of a data type, or return None."""
    try:
        return cls()
    except TypeError:
        pass
    kwargs = PARAMETERIZED_ARGS.get(name)
    if kwargs is None:
        return None
    try:
        return cls(**kwargs)
    except Exception:  # noqa: BLE001 - best effort; reported as skipped below
        return None


def main():
    entries = []
    skipped = []

    for name, cls in data_type_registry.contents.items():
        instance = instantiate(name, cls)
        if instance is None:
            skipped.append(name)
            continue
        try:
            data_type_json = instance.to_json(zarr_format=3)
            fill_value_json = instance.to_json_scalar(
                instance.default_scalar(), zarr_format=3
            )
        except Exception as exc:  # noqa: BLE001
            skipped.append(f"{name} ({type(exc).__name__})")
            continue

        entries.append(
            {
                "name": name,
                "data_type": data_type_json,
                "fill_value": fill_value_json,
            }
        )

    document = {
        "_comment": (
            "GENERATED FILE -- do not edit by hand. Regenerate with "
            "'uv run src/test/python-scripts/generate_spec_data_types.py'. "
            "This is the external conformance list for "
            "DataTypeConformanceTest; it is intentionally not derived from "
            "zarr-java's DataType enum."
        ),
        "source": f"zarr-python {zarr.__version__} data_type_registry",
        "data_types": entries,
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(document, indent=2, sort_keys=False) + "\n")

    print(f"wrote {OUT_PATH} with {len(entries)} data types "
          f"(zarr-python {zarr.__version__})")
    if skipped:
        print(f"skipped (could not instantiate): {', '.join(skipped)}",
              file=sys.stderr)


if __name__ == "__main__":
    main()
