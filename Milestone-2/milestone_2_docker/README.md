# DataFusion Comet — TPC-H Demo
### Spark 3.5.8 + Comet 0.14.0

## Requirements
- Docker Desktop (Mac/Windows) or Docker Engine (Linux)
- At 10 GB disk space recommended
- Prebuilt 0.14.0 Comet Spark + Common Jars in working directory
- Shell scripts in /scripts within working directory

## Quick start

```bash
# Step 1: build the image (may take 30+ min)
docker compose build

# Step 2: run the benchmarks
docker compose run comet-demo bash /opt/scripts/run_demo.sh
```

Open **http://localhost:4040** while queries are running to see the Spark UI.

## What the build does
1. Installs Java 17, Rust, Maven
2. Downloads Spark 3.5.8
3. Builds Comet 0.14.0 from prebuilt JARs
4. Compiles `tpch-dbgen` and generates TPC-H SF=1 data
5. Converts `.tbl` files to Parquet

All subsequent `docker compose run` calls start instantly from the cached image.

## Current Issues
As of 3/25, the custom SMJ flag does not appear to be contributing to a speed up in regards to Q21. Due to the complexity of the build for this project, its difficult to pinpoint/debug what the exact issue is, especially with the current given timeframe. So currently, the demo script runs the TPC-H benchmarks twice, once with the traditional/legacy SMJ flag enabled and once with the custom SMJ flag enabled, but there likely won't be significant improvement with the custom SMJ run until this issue is fixed.

