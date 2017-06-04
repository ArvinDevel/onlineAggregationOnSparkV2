/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import scala.collection.mutable.ListBuffer

// every impl class should use confidence and errorBound as parameter

class OnlineSum(confidence: Double, errorBound: Double, size: Long)
  extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = {
    new StructType().add("myinput", DoubleType)
  }

  override def bufferSchema: StructType = {
    new StructType().add("mycnt", LongType).add("mysum", DoubleType)
  }

  override def dataType: DataType = StringType


  override def deterministic: Boolean = true


  override def initialize(buffer: MutableAggregationBuffer): Unit = {

    buffer.update(0, 0L)
    buffer.update(1, 0d)
  }


  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, buffer.getAs[Long](0) + 1)

    buffer.update(1, buffer.getAs[Double](1) + input.getAs[Double](0))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.getAs[Long](0) + buffer2.getAs[Long](0))
    buffer1.update(1, buffer1.getAs[Double](1) + buffer2.getAs[Double](1))

  }

  override def evaluate(buffer: Row): Any = {
    val avg = buffer.getAs[Double](1) / buffer.getAs[Long](0)
    s"$avg%.2f\tP=0.2\terrorBound=0.01".toString
  }


}

class OnlineCount(confidence: Double, errorBound: Double, size: Long)
  extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = {
    new StructType().add("myinput", DoubleType)
  }

  override def bufferSchema: StructType = {
    new StructType().add("mycnt", LongType).add("mysum", DoubleType)
  }

  override def dataType: DataType = StringType


  override def deterministic: Boolean = true


  override def initialize(buffer: MutableAggregationBuffer): Unit = {

    buffer.update(0, 0L)
    buffer.update(1, 0d)
  }


  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, buffer.getAs[Long](0) + 1)

    buffer.update(1, buffer.getAs[Double](1) + input.getAs[Double](0))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.getAs[Long](0) + buffer2.getAs[Long](0))
    buffer1.update(1, buffer1.getAs[Double](1) + buffer2.getAs[Double](1))

  }

  override def evaluate(buffer: Row): Any = {
    val avg = buffer.getAs[Double](1) / buffer.getAs[Long](0)
    s"$avg%.2f\tP=0.2\terrorBound=0.01".toString
  }


}

class OnlineMin(confidence: Double, errorBound: Double, size: Long) extends
  UserDefinedAggregateFunction {
  override def inputSchema: StructType = {
    new StructType().add("myinput", DoubleType)
  }

  override def bufferSchema: StructType = {
    new StructType().add("mycnt", LongType).add("mysum", DoubleType)
  }

  override def dataType: DataType = StringType


  override def deterministic: Boolean = true


  override def initialize(buffer: MutableAggregationBuffer): Unit = {

    buffer.update(0, 0L)
    buffer.update(1, 0d)
  }


  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, buffer.getAs[Long](0) + 1)

    buffer.update(1, buffer.getAs[Double](1) + input.getAs[Double](0))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.getAs[Long](0) + buffer2.getAs[Long](0))
    buffer1.update(1, buffer1.getAs[Double](1) + buffer2.getAs[Double](1))

  }

  override def evaluate(buffer: Row): Any = {
    val avg = buffer.getAs[Double](1) / buffer.getAs[Long](0)
    s"$avg%.2f\tP=0.2\terrorBound=0.01".toString
  }


}

class OnlineMax(confidence: Double, errorBound: Double, size: Long)
  extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = {
    new StructType().add("myinput", DoubleType)
  }

  override def bufferSchema: StructType = {
    new StructType().add("mycnt", LongType).add("mysum", DoubleType)
  }

  override def dataType: DataType = StringType


  override def deterministic: Boolean = true


  override def initialize(buffer: MutableAggregationBuffer): Unit = {

    buffer.update(0, 0L)
    buffer.update(1, 0d)
  }


  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, buffer.getAs[Long](0) + 1)

    buffer.update(1, buffer.getAs[Double](1) + input.getAs[Double](0))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.getAs[Long](0) + buffer2.getAs[Long](0))
    buffer1.update(1, buffer1.getAs[Double](1) + buffer2.getAs[Double](1))

  }

  override def evaluate(buffer: Row): Any = {
    val avg = buffer.getAs[Double](1) / buffer.getAs[Long](0)
    s"$avg%.2f\tP=0.2\terrorBound=0.01".toString
  }

}

class OnlineAvg(confidence: Double, errorBound: Double, size: Long) extends
  UserDefinedAggregateFunction {

  // Input Data Type Schema.
  // Assuming aggregate on single column, and its type is DoubleType.
  override def inputSchema: StructType = {
    new StructType().add("execColumn", DoubleType)
  }

  // Intermediate Schema
  override def bufferSchema: StructType = {
    new StructType().add("count", LongType).add("sum", DoubleType)
  }

  // Return type
  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  private val batchSize = 100
  private var batch = new ListBuffer[Double]()
  private var batchPivot = 0
  private var historicalAvg = 0d
  private var historicalVar = 0d
  private var crtCount = 0L
  private var crtSum = 0d

  def updateHistorical(): Unit = {
    val crtAvg = crtSum / crtCount

    val batchAvg: Double = batch.sum / batch.length
    val batchVar: Double = calcBatchVar()
    val historicalCount = crtCount - batch.length

    historicalVar = if (historicalCount == 0) batchVar
    else (
      historicalCount * (historicalVar + math.pow(crtAvg - historicalAvg, 2.0)) +
        batchSize * (batchVar + math.pow(crtAvg - batchAvg, 2.0))
      ) / (historicalCount + batchSize)

    historicalAvg = if (historicalCount == 0) batchAvg
    else (crtSum - batch.sum) / (crtCount - batch.length)
  }

  def calcBatchVar(): Double = {
    val batchAvg: Double = batch.sum / batch.length
    batch.foldLeft(0d) { case (sum, sample) =>
      sum + (sample - batchAvg) * (sample - batchAvg)
    } / batch.length
  }

  // Initialize the Intermediate buffer
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, 0L)
    buffer.update(1, 0d)
  }


  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, buffer.getAs[Long](0) + 1)
    crtCount = buffer.getAs[Long](0)
    buffer.update(1, buffer.getAs[Double](1) + input.getAs[Double](0))
    crtSum = buffer.getAs[Double](1)

    if (batchPivot < batchSize) {
      batch += input.getAs[Double](0)
      batchPivot += 1
    } else {
      updateHistorical()
      batch.clear()
      batchPivot = 0
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.getAs[Long](0) + buffer2.getAs[Long](0))
    buffer1.update(1, buffer1.getAs[Double](1) + buffer2.getAs[Double](1))
  }

  override def evaluate(buffer: Row): Any = {
    val avg = buffer.getAs[Double](1) / buffer.getAs[Long](0)

    updateHistorical()

    val T = math.sqrt(historicalVar)
    val errorBound: Double = 2.0
    val confidence = commonMath.calcConfidence(errorBound, crtCount, T)

    s"runningResult=$avg\tP=$confidence\terrorBound=$errorBound".toString
  }
}