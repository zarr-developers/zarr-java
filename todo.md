
● Here's a summary you can paste:

  ---
  Task: Extract the OME metadata classes from zarr-java into a separate Maven module (zarr-java-ome) within the same repo.

  What was done:
  - Converted the repo to a Maven multi-module project:
    - Root pom.xml → pure aggregator (packaging=pom), lists both modules
    - zarr-java-core/ → contains the original src/ and testdata/ (everything except OME)
    - zarr-java-ome/ → new module with OME source, tests, and testdata
  - zarr-java-core/pom.xml — all original dependencies + maven-jar-plugin producing a test-jar so OME tests can extend ZarrTest
  - zarr-java-ome/pom.xml — depends on zarr-java-core + zarr-java-core:test-jar (test scope)
  - GPG signing moved into a release Maven profile so mvn install works locally without a key

  Current state:
  - mvn install -DskipTests → BUILD SUCCESS
  - Core tests (430) → all pass
  - OME v0.4 and v0.5 tests → pass
  - OME v0.6 tests → failing because testdata/ome/v0.6/examples is a git submodule pointing to https://github.com/jo-mueller/ngff-rfc5-coordinate-transformation-examples — it was moved into
  zarr-java-ome/testdata/ but the submodule reference in .gitmodules was not updated, so the folder is empty
  - Working on branch extract-ome-module

  Next steps:
  - Fix the git submodule: either update .gitmodules to point to zarr-java-ome/testdata/ome/v0.6/examples, or keep the submodule at the repo root and reference it from there
  - Run git submodule update --init to populate the submodule
  - Verify all OME tests pass
  - Update CI/CD deploy workflow if needed

