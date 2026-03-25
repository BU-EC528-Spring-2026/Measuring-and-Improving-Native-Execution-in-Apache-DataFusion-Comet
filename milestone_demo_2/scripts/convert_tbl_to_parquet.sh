#!/bin/bash
set -e
SPARK_HOME=/opt/spark
COMET_JAR=/opt/comet-spark.jar
TBL_DIR=/opt/tpch-dbgen/data/sf1
PARQUET_DIR=/opt/tpch-dbgen/data/sf1_parquet

echo "Converting TPC-H .tbl -> Parquet..."

$SPARK_HOME/bin/spark-shell --driver-memory 4g \
  --jars "$COMET_JAR" \
  --conf spark.driver.extraClassPath="$COMET_JAR" \
  --conf spark.plugins=org.apache.spark.CometPlugin \
  --conf spark.comet.enabled=true \
  << 'SCALA'
import org.apache.spark.sql.types._
import java.io.File

val tblDir     = "/opt/tpch-dbgen/data/sf1"
val parquetDir = "/opt/tpch-dbgen/data/sf1_parquet"
new File(parquetDir).mkdirs()

val schemas = Map(
  "customer"  -> StructType(Seq(StructField("c_custkey",IntegerType),StructField("c_name",StringType),StructField("c_address",StringType),StructField("c_nationkey",IntegerType),StructField("c_phone",StringType),StructField("c_acctbal",DoubleType),StructField("c_mktsegment",StringType),StructField("c_comment",StringType))),
  "lineitem"  -> StructType(Seq(StructField("l_orderkey",IntegerType),StructField("l_partkey",IntegerType),StructField("l_suppkey",IntegerType),StructField("l_linenumber",IntegerType),StructField("l_quantity",DoubleType),StructField("l_extendedprice",DoubleType),StructField("l_discount",DoubleType),StructField("l_tax",DoubleType),StructField("l_returnflag",StringType),StructField("l_linestatus",StringType),StructField("l_shipdate",DateType),StructField("l_commitdate",DateType),StructField("l_receiptdate",DateType),StructField("l_shipinstruct",StringType),StructField("l_shipmode",StringType),StructField("l_comment",StringType))),
  "orders"    -> StructType(Seq(StructField("o_orderkey",IntegerType),StructField("o_custkey",IntegerType),StructField("o_orderstatus",StringType),StructField("o_totalprice",DoubleType),StructField("o_orderdate",DateType),StructField("o_orderpriority",StringType),StructField("o_clerk",StringType),StructField("o_shippriority",IntegerType),StructField("o_comment",StringType))),
  "part"      -> StructType(Seq(StructField("p_partkey",IntegerType),StructField("p_name",StringType),StructField("p_mfgr",StringType),StructField("p_brand",StringType),StructField("p_type",StringType),StructField("p_size",IntegerType),StructField("p_container",StringType),StructField("p_retailprice",DoubleType),StructField("p_comment",StringType))),
  "partsupp"  -> StructType(Seq(StructField("ps_partkey",IntegerType),StructField("ps_suppkey",IntegerType),StructField("ps_availqty",IntegerType),StructField("ps_supplycost",DoubleType),StructField("ps_comment",StringType))),
  "supplier"  -> StructType(Seq(StructField("s_suppkey",IntegerType),StructField("s_name",StringType),StructField("s_address",StringType),StructField("s_nationkey",IntegerType),StructField("s_phone",StringType),StructField("s_acctbal",DoubleType),StructField("s_comment",StringType))),
  "nation"    -> StructType(Seq(StructField("n_nationkey",IntegerType),StructField("n_name",StringType),StructField("n_regionkey",IntegerType),StructField("n_comment",StringType))),
  "region"    -> StructType(Seq(StructField("r_regionkey",IntegerType),StructField("r_name",StringType),StructField("r_comment",StringType)))
)

schemas.foreach { case (name, schema) =>
  println(s"  Converting $name...")
  val raw     = spark.read.option("delimiter","|").option("header","false").csv(s"$tblDir/$name.tbl")
  val trimmed = raw.drop(raw.columns.last)
  trimmed.toDF(schema.fieldNames: _*).write.mode("overwrite").parquet(s"$parquetDir/${name}_parquet")
  println(s"  Done: $name")
}
println("All tables converted.")
System.exit(0)
SCALA

echo "Parquet conversion complete -> $PARQUET_DIR"
