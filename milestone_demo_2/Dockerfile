FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_VERSION=3.5.8
ENV SPARK_HOME=/opt/spark
# ENV COMET_VERSION=0.15.0-SNAPSHOT
ENV PATH=$SPARK_HOME/bin:$JAVA_HOME/bin:/root/.cargo/bin:$PATH

# ── System deps ───────────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    wget curl git \
    gcc g++ make pkg-config \
    libssl-dev \
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

# ── Comet 0.14.0-SNAPSHOT built from JAR ───────────────────────────────────
# Use pre-built JAR w/ TPC-H fixes
COPY comet-spark-spark3.5_2.12-0.14.0-SNAPSHOT.jar /opt/comet-spark.jar
RUN echo "Comet JAR ready: /opt/comet-spark.jar"

# ── Comet 0.15.0-SNAPSHOT built from source ───────────────────────────────────
# Builds the unmodified main branch (no fix applied).
# RUN git clone https://github.com/apache/datafusion-comet.git /opt/datafusion-comet \
#     && cd /opt/datafusion-comet \
#     && git checkout main \
#     && mvn clean package -Pspark-3.5 -DskipTests -pl spark -am \
#     && cp spark/target/comet-spark-spark3.5_2.12-${COMET_VERSION}.jar /opt/comet-spark.jar \
#     && echo "Comet JAR ready: /opt/comet-spark.jar"

# ── TPC-H data generator ──────────────────────────────────────────────────────
RUN git clone https://github.com/electrum/tpch-dbgen.git /opt/tpch-dbgen \
    && cd /opt/tpch-dbgen \
    && make \
    && mkdir -p data/sf1 \
    && cd data/sf1 \
    && ../../dbgen -s 1 -f \
    && echo "TPC-H SF=1 .tbl files generated"

# ── Scripts ───────────────────────────────────────────────────────────────────
COPY scripts/ /opt/scripts/
RUN chmod +x /opt/scripts/*.sh

# Convert .tbl → Parquet at build time so the container starts ready to query
RUN /opt/scripts/convert_tbl_to_parquet.sh

WORKDIR /opt
EXPOSE 4040
CMD ["/bin/bash"]
