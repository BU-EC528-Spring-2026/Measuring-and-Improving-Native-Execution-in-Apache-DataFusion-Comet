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

package org.apache.spark.sql.comet.execution.arrow

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.columnar.{CachedBatch, CachedBatchSerializer}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.io.ChunkedByteBuffer

import org.apache.comet.CometConf

private case class CometCachedBatch(
    override val numRows: Int,
    override val sizeInBytes: Long,
    stats: InternalRow,
    bytes: ChunkedByteBuffer)
    extends CachedBatch

class ArrowCachedBatchSerializer extends CachedBatchSerializer {

  private val fallback = new DefaultCachedBatchSerializer()

  private def enabled(conf: SQLConf): Boolean = {
    CometConf.COMET_ENABLED.get(conf) &&
    CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.get(conf)
  }

  private def toStructType(schema: Seq[Attribute]): StructType = {
    StructType(schema.map { attr =>
      StructField(attr.name, attr.dataType, attr.nullable, attr.metadata)
    })
  }

  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = true

  override def supportsColumnarOutput(schema: StructType): Boolean = true

  override def convertColumnarBatchToCachedBatch(
      input: RDD[ColumnarBatch],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {

    if (!enabled(conf)) {
      fallback.convertColumnarBatchToCachedBatch(input, schema, storageLevel, conf)
    } else {
      input.mapPartitions { batches =>
        Utils.serializeBatches(batches).map { case (rows, buffer) =>
          CometCachedBatch(
            numRows = rows.toInt,
            sizeInBytes = buffer.size,
            stats = InternalRow.empty,
            bytes = buffer)
        }
      }
    }
  }

  override def convertCachedBatchToColumnarBatch(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch] = {

    if (!enabled(conf)) {
      fallback.convertCachedBatchToColumnarBatch(input, cacheAttributes, selectedAttributes, conf)
    } else {
      val selectedIndices =
        if (selectedAttributes.isEmpty) {
          cacheAttributes.indices.toArray
        } else {
          val byExprId = cacheAttributes.zipWithIndex.map { case (attr, idx) =>
            attr.exprId -> idx
          }.toMap

          selectedAttributes.map { attr =>
            byExprId.getOrElse(
              attr.exprId,
              throw new IllegalStateException(
                s"Could not resolve selected attribute ${attr.name} from cache attributes"))
          }.toArray
        }

      input.mapPartitions { it =>
        it.flatMap {
          case cb: CometCachedBatch =>
            Utils.decodeBatches(cb.bytes, "CometCache").map { batch =>
              if (selectedIndices.length == batch.numCols()) {
                batch
              } else {
                val cols = selectedIndices.map(i => batch.column(i).asInstanceOf[ColumnVector])
                new ColumnarBatch(cols, batch.numRows())
              }
            }

          case other =>
            throw new IllegalStateException(
              s"Expected CometCachedBatch, got ${other.getClass.getName}")
        }
      }
    }
  }

  override def convertInternalRowToCachedBatch(
      input: RDD[InternalRow],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {

    if (!enabled(conf)) {
      fallback.convertInternalRowToCachedBatch(input, schema, storageLevel, conf)
    } else {
      val batchSize = conf.columnBatchSize
      val sessionTz = conf.sessionLocalTimeZone

      input.mapPartitions { rows =>
        val iter = CometArrowConverters.rowToArrowBatchIter(
          rows,
          toStructType(schema),
          batchSize,
          sessionTz,
          TaskContext.get())

        Utils.serializeBatches(iter).map { case (rows, buffer) =>
          CometCachedBatch(
            numRows = rows.toInt,
            sizeInBytes = buffer.size,
            stats = InternalRow.empty,
            bytes = buffer)
        }
      }
    }
  }

  override def convertCachedBatchToInternalRow(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[InternalRow] = {
    if (!enabled(conf)) {
      fallback.convertCachedBatchToInternalRow(input, cacheAttributes, selectedAttributes, conf)
    } else {
      throw new UnsupportedOperationException("Row fallback not supported in Comet cache mode")
    }
  }

  override def buildFilter(
      predicates: Seq[Expression],
      cachedAttributes: Seq[Attribute]): (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = {
    (partitionIndex: Int, it: Iterator[CachedBatch]) => it
  }
}
