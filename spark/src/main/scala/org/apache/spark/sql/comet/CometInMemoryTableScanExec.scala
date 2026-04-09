/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.comet

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.columnar.{CachedBatch, CachedBatchSerializer}
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.CometConf
import org.apache.comet.serde.CometOperatorSerde
import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.serializeDataType

case class CometInMemoryTableScanExec(
    originalPlan: InMemoryTableScanExec,
    serializer: CachedBatchSerializer,
    cachedBuffers: RDD[CachedBatch],
    relationOutput: Seq[Attribute],
    scanOutput: Seq[Attribute])
    extends CometExec
    with LeafExecNode {

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def output: Seq[Attribute] = originalPlan.output

  override def vectorTypes: Option[Seq[String]] =
    serializer.vectorTypes(scanOutput, conf)

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")

    serializer
      .convertCachedBatchToColumnarBatch(cachedBuffers, relationOutput, scanOutput, conf)
      .map { cb =>
        numOutputRows += cb.numRows()
        cb
      }
  }
}

object CometInMemoryTableScanExec extends CometOperatorSerde[InMemoryTableScanExec] {

  override def enabledConfig: Option[org.apache.comet.ConfigEntry[Boolean]] =
    Some(CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED)

  override def convert(
      op: InMemoryTableScanExec,
      builder: OperatorOuterClass.Operator.Builder,
      childOp: Operator*): Option[Operator] = {

    val actualOutput =
      if (op.output.nonEmpty) op.output
      else op.relation.output

    val scanTypes = actualOutput.flatMap(attr => serializeDataType(attr.dataType))

    val scanBuilder = OperatorOuterClass.Scan
      .newBuilder()
      .setSource(op.getClass.getSimpleName)
      .addAllFields(scanTypes.asJava)
      .setArrowFfiSafe(false)

    Some(builder.setScan(scanBuilder).build())
  }

  override def createExec(nativeOp: Operator, op: InMemoryTableScanExec): CometNativeExec = {
    val relation = op.relation

    val actualOutput =
      if (op.output.nonEmpty) op.output
      else relation.output

    CometScanWrapper(
      nativeOp,
      CometInMemoryTableScanExec(
        op,
        relation.cacheBuilder.serializer,
        relation.cacheBuilder.cachedColumnBuffers,
        relation.output,
        actualOutput))
  }
}
