#!/bin/bash
set -e
SPARK_HOME=${SPARK_HOME:-/opt/spark}
COMET_SPARK_JAR=${COMET_SPARK_JAR:-/opt/comet-spark.jar}
COMET_COMMON_JAR=${COMET_COMMON_JAR:-/opt/comet-common.jar}
TBL_DIR=${TBL_DIR:-/opt/tpch-dbgen/data/sf1}
PARQUET_DIR=${PARQUET_DIR:-/opt/tpch-dbgen/data/sf1_parquet}

echo "Converting TPC-H .tbl -> Parquet..."

$SPARK_HOME/bin/spark-shell --driver-memory 4g \
  --jars "$COMET_SPARK_JAR,$COMET_COMMON_JAR" \
  --conf spark.driver.extraClassPath="$COMET_SPARK_JAR:$COMET_COMMON_JAR" \
  --conf spark.plugins=org.apache.spark.CometPlugin \
  --conf spark.comet.enabled=true \
  << 'SCALA'
import org.apache.spark.sql.types._
import java.io.File

val tblDir     = "/opt/tpch-dbgen/data/sf1"
val parquetDir = "/opt/tpch-dbgen/data/sf1_parquet"
new File(parquetDir).mkdirs()

val env = sys.env
val resolvedTblDir = env.getOrElse("TBL_DIR", tblDir)
val resolvedParquetDir = env.getOrElse("PARQUET_DIR", parquetDir)
new File(resolvedParquetDir).mkdirs()

val customerSchema = StructType(Seq(
  StructField("c_custkey", IntegerType, true),
  StructField("c_name", StringType, true),
  StructField("c_address", StringType, true),
  StructField("c_nationkey", IntegerType, true),
  StructField("c_phone", StringType, true),
  StructField("c_acctbal", DoubleType, true),
  StructField("c_mktsegment", StringType, true),
  StructField("c_comment", StringType, true)
))

val lineitemSchema = StructType(Seq(
  StructField("l_orderkey", IntegerType, true),
  StructField("l_partkey", IntegerType, true),
  StructField("l_suppkey", IntegerType, true),
  StructField("l_linenumber", IntegerType, true),
  StructField("l_quantity", DoubleType, true),
  StructField("l_extendedprice", DoubleType, true),
  StructField("l_discount", DoubleType, true),
  StructField("l_tax", DoubleType, true),
  StructField("l_returnflag", StringType, true),
  StructField("l_linestatus", StringType, true),
  StructField("l_shipdate", DateType, true),
  StructField("l_commitdate", DateType, true),
  StructField("l_receiptdate", DateType, true),
  StructField("l_shipinstruct", StringType, true),
  StructField("l_shipmode", StringType, true),
  StructField("l_comment", StringType, true)
))

val ordersSchema = StructType(Seq(
  StructField("o_orderkey", IntegerType, true),
  StructField("o_custkey", IntegerType, true),
  StructField("o_orderstatus", StringType, true),
  StructField("o_totalprice", DoubleType, true),
  StructField("o_orderdate", DateType, true),
  StructField("o_orderpriority", StringType, true),
  StructField("o_clerk", StringType, true),
  StructField("o_shippriority", IntegerType, true),
  StructField("o_comment", StringType, true)
))

val partSchema = StructType(Seq(
  StructField("p_partkey", IntegerType, true),
  StructField("p_name", StringType, true),
  StructField("p_mfgr", StringType, true),
  StructField("p_brand", StringType, true),
  StructField("p_type", StringType, true),
  StructField("p_size", IntegerType, true),
  StructField("p_container", StringType, true),
  StructField("p_retailprice", DoubleType, true),
  StructField("p_comment", StringType, true)
))

val partsuppSchema = StructType(Seq(
  StructField("ps_partkey", IntegerType, true),
  StructField("ps_suppkey", IntegerType, true),
  StructField("ps_availqty", IntegerType, true),
  StructField("ps_supplycost", DoubleType, true),
  StructField("ps_comment", StringType, true)
))

val supplierSchema = StructType(Seq(
  StructField("s_suppkey", IntegerType, true),
  StructField("s_name", StringType, true),
  StructField("s_address", StringType, true),
  StructField("s_nationkey", IntegerType, true),
  StructField("s_phone", StringType, true),
  StructField("s_acctbal", DoubleType, true),
  StructField("s_comment", StringType, true)
))

val nationSchema = StructType(Seq(
  StructField("n_nationkey", IntegerType, true),
  StructField("n_name", StringType, true),
  StructField("n_regionkey", IntegerType, true),
  StructField("n_comment", StringType, true)
))

val regionSchema = StructType(Seq(
  StructField("r_regionkey", IntegerType, true),
  StructField("r_name", StringType, true),
  StructField("r_comment", StringType, true)
))

val tableInfo = Seq(
  "customer" -> customerSchema,
  "lineitem" -> lineitemSchema,
  "orders" -> ordersSchema,
  "part" -> partSchema,
  "partsupp" -> partsuppSchema,
  "supplier" -> supplierSchema,
  "nation" -> nationSchema,
  "region" -> regionSchema
)

tableInfo.foreach { case (name, schema) =>
  println(s"Processing $name...")
  val df = spark.read
    .option("delimiter", "|")
    .option("header", "false")
    .schema(schema)
    .csv(s"$resolvedTblDir/$name.tbl")

  // Only drop the last column if it's empty (caused by trailing pipe in some .tbl files)
  val dfCleaned = if (df.columns.length > schema.length) {
    println(s"  -> Dropping empty trailing column")
    df.drop(df.columns.last)
  } else {
    df
  }

  // write Parquet with properly typed columns
  dfCleaned.write.mode("overwrite").parquet(s"$resolvedParquetDir/${name}_parquet")
  println(s"Written $name to Parquet with correct schema")
}

println("All tables converted.")
System.exit(0)
SCALA

echo "Parquet conversion complete -> $PARQUET_DIR"