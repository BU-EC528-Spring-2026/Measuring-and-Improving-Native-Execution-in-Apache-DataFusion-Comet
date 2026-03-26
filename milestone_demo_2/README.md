# DataFusion Comet — TPC-H Demo
### Spark 3.5.8 + Comet 0.14.0

## Requirements
- Docker Desktop (Mac/Windows) or Docker Engine (Linux)
- At 10 GB disk space recommended
- Prebuilt 0.14.0 Comet Spark + Common Jars in working directory

## Quick start

```bash
# Step 1: build the image (may take 30+ min)
docker compose build

# Step 2: run the benchmark
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
