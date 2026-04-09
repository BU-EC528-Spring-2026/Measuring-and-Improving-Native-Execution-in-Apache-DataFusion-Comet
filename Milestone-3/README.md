---

# 🚀 Comet Build & Run Instructions

## Clone the Repository

```bash
git clone -b pchintar-course-clean https://github.com/BU-EC528-Spring-2026/Measuring-and-Improving-Native-Execution-in-Apache-DataFusion-Comet.git
cd Measuring-and-Improving-Native-Execution-in-Apache-DataFusion-Comet
```

---

## Requirements (Install First)

### Java 17

**macOS**

```bash
brew install openjdk@17
```

**Linux**

```bash
sudo apt update && sudo apt install openjdk-17-jdk -y
```

---

### Apache Spark 3.5.8

```bash
wget https://archive.apache.org/dist/spark/spark-3.5.8/spark-3.5.8-bin-hadoop3.tgz
tar -xzf spark-3.5.8-bin-hadoop3.tgz
export SPARK_HOME=$PWD/spark-3.5.8-bin-hadoop3
```

---

### Maven

**macOS**

```bash
brew install maven
```

**Linux**

```bash
sudo apt install maven -y
```

---

### Rust + Cargo

```bash
curl https://sh.rustup.rs -sSf | sh
source $HOME/.cargo/env
```

---

## Build + Package Comet

```bash
cd native
cargo build --release
cd ..

./mvnw clean
./mvnw install -DskipTests -Drat.skip=true

export COMET_JAR=$(ls spark/target/comet-spark-spark3.5_2.12-*.jar | grep -v sources | head -n 1)
```

---

## Embed Native Library (choose ONE based on OS)

### macOS (Intel)

```bash
mkdir -p common/target/classes/org/apache/comet/darwin/x86_64
cp native/target/release/libcomet.dylib common/target/classes/org/apache/comet/darwin/x86_64/
jar uf "$COMET_JAR" -C common/target/classes org/apache/comet/darwin/x86_64/libcomet.dylib
```

### macOS (Apple Silicon)

```bash
mkdir -p common/target/classes/org/apache/comet/darwin/aarch64
cp native/target/release/libcomet.dylib common/target/classes/org/apache/comet/darwin/aarch64/
jar uf "$COMET_JAR" -C common/target/classes org/apache/comet/darwin/aarch64/libcomet.dylib
```

### Linux (recommended for graders)

```bash
mkdir -p common/target/classes/org/apache/comet/linux/x86_64
cp native/target/release/libcomet.so common/target/classes/org/apache/comet/linux/x86_64/
jar uf "$COMET_JAR" -C common/target/classes org/apache/comet/linux/x86_64/libcomet.so
```

---

## Verify

```bash
jar tf "$COMET_JAR" | grep libcomet
```

Expected output:

```
org/apache/comet/<platform>/<arch>/libcomet.*
```

---

## Run Spark with Comet

```bash
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java)))) 2>/dev/null || export JAVA_HOME=$(/usr/libexec/java_home -v 17)

$SPARK_HOME/bin/spark-shell \
  --jars "$COMET_JAR" \
  --conf spark.driver.extraClassPath="$COMET_JAR" \
  --conf spark.executor.extraClassPath="$COMET_JAR" \
  --conf spark.plugins=org.apache.spark.CometPlugin \
  --conf spark.comet.enabled=true \
  --conf spark.comet.explainFallback.enabled=true \
  --conf spark.comet.exec.inMemoryCache.enabled=true \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=16g \
  --conf spark.comet.exec.localTableScan.enabled=true \
  --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager
```

---

## Notes

* macOS → `.dylib`, Linux → `.so`
* Mismatch → `UnsatisfiedLinkError`
* Linux path recommended for graders/cloud
* Tested on macOS; Linux instructions provided

---

## Result

Comet is built, native library embedded, and Spark runs with Comet enabled.
