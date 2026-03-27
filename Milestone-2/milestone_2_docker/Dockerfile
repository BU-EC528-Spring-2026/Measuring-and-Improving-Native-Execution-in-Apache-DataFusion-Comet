FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_VERSION=3.5.8
ENV SPARK_HOME=/opt/spark
# ENV COMET_VERSION=0.14.0
ENV PATH=$SPARK_HOME/bin:$JAVA_HOME/bin:/root/.cargo/bin:$PATH

# ── System deps ───────────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    wget curl git \
    gcc g++ make pkg-config \
    libssl-dev \
    protobuf-compiler \
    python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*

# ── Rust ─────────────────────────────────────────────────────────────────────
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# ── Maven ─────────────────────────────────────────────────────────────────────
RUN wget -q https://archive.apache.org/dist/maven/maven-3/3.9.6/binaries/apache-maven-3.9.6-bin.tar.gz \
    && tar -xzf apache-maven-3.9.6-bin.tar.gz -C /opt \
    && ln -s /opt/apache-maven-3.9.6/bin/mvn /usr/local/bin/mvn \
    && rm apache-maven-3.9.6-bin.tar.gz

# ── Spark 3.5.8 ───────────────────────────────────────────────────────────────
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark \
    && rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# ── Datafusion 52.3.0 ───────────────────────────────────────────────────────────────
# Clone the DataFusion repo and apply the patch with the TPC-H fixes. We will build this patched version of DataFusion from source when we build Comet.
# COPY datafusion-changes.patch /tmp/
# RUN git clone https://github.com/apache/datafusion.git /opt/datafusion \
#     && cd /opt/datafusion \
#     && git checkout 52.3.0 \
#     && git apply /tmp/datafusion-changes.patch

# ── Comet 0.14.0 built from JAR ───────────────────────────────────
# Use pre-built JARs w/ TPC-H fixes
# COPY comet-spark-linux.jar /opt/comet-spark.jar
COPY comet-spark-fixed.jar /opt/comet-spark.jar
# COPY comet-spark-spark3.5_2.12-0.14.0.jar /opt/comet-spark.jar
# COPY comet-common-spark3.5_2.12-0.14.0.jar /opt/comet-common.jar
# RUN echo "Comet Spark JAR ready: /opt/comet-spark.jar"
# RUN echo "Comet common JAR ready: /opt/comet-common.jar"

# # ── Comet 0.14.0-SNAPSHOT built from source ───────────────────────────────────
# # Builds Comet from source with the TPC-H fixes
# COPY datafusion-comet-changes.patch /tmp/
# RUN git clone https://github.com/apache/datafusion-comet.git /opt/datafusion-comet \
#     && cd /opt/datafusion-comet \
#     && git checkout 0.14.0 \
#     && git apply /tmp/datafusion-comet-changes.patch \
#     && sed -i 's|/root/datafusion|/opt/datafusion|g' /opt/datafusion-comet/native/Cargo.toml

# # Patch Comet's Cargo.toml to use the local patched DataFusion instead of pulling from crates.io. This ensures that when we build Comet from source, it incorporates our DataFusion changes.
# RUN mkdir -p /opt/datafusion-comet/native/.cargo \
#     && cat > /opt/datafusion-comet/native/.cargo/config.toml << 'EOF'
# [patch.crates-io]
# datafusion = { path = "/opt/datafusion/datafusion/core" }
# datafusion-physical-plan = { path = "/opt/datafusion/datafusion/physical-plan" }
# datafusion-execution = { path = "/opt/datafusion/datafusion/execution" }
# datafusion-expr = { path = "/opt/datafusion/datafusion/expr" }
# datafusion-common = { path = "/opt/datafusion/datafusion/common" }
# datafusion-physical-expr = { path = "/opt/datafusion/datafusion/physical-expr" }
# EOF

# # ── Protobuf compiler ───────────────────────────────────────────────────────
# RUN apt-get update && apt-get install -y unzip \
#     && wget -q https://github.com/protocolbuffers/protobuf/releases/download/v33.2/protoc-33.2-linux-x86_64.zip \
#     && unzip protoc-33.2-linux-x86_64.zip -d /usr/local \
#     && rm protoc-33.2-linux-x86_64.zip

# # ── Build Comet native code and JAR ─────────────────────────────────────────
# RUN cd /opt/datafusion-comet/native \
#     && cargo build --release

# RUN cd /opt/datafusion-comet \
#     && mvn clean package -Pspark-3.5 -DskipTests -pl spark -am \
#     && cp spark/target/comet-spark-spark3.5_2.12-${COMET_VERSION}.jar /opt/comet-spark.jar \
#     && echo "Comet JAR ready: /opt/comet-spark.jar"

# ── TPC-H data generator ──────────────────────────────────────────────────────
RUN git clone https://github.com/electrum/tpch-dbgen.git /opt/tpch-dbgen \
    && cd /opt/tpch-dbgen \
    && make \
    && ./dbgen -s 1 -f \
    && mkdir -p data/sf1 \
    && mv *.tbl data/sf1/ \
    && echo "TPC-H SF=1 .tbl files generated"

# ── Scripts ───────────────────────────────────────────────────────────────────
COPY scripts/ /opt/scripts/
RUN sed -i 's/\r$//' /opt/scripts/*.sh \
    && chmod +x /opt/scripts/*.sh

# Convert .tbl → Parquet at build time so the container starts ready to query
RUN /opt/scripts/convert_tbl_to_parquet.sh

WORKDIR /opt
EXPOSE 4040
CMD ["/bin/bash"]
