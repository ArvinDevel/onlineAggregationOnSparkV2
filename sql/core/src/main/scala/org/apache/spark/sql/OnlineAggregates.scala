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

import scala.util.control._
import org.apache.spark.Logging
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

// every impl class should use confidence and errorBound as parameter


class OnlineSum(confidence: Double, errorBound: Double, size: Long)
  extends UserDefinedAggregateFunction {

  private val batchSize = 100
  private var batch = new ListBuffer[Double]()
  private var batchPivot = 0
  private var historicalAvg = 0d
  private var historicalVar = 0d
  private var crtCount = 0L
  private var crtSum = 0d
  private var tableSizeSqrt: Double = size // sqrt of the table

  override def inputSchema: StructType = {
    new StructType().add("myinput", DoubleType)
  }

  override def bufferSchema: StructType = {
    new StructType().add("mycnt", LongType).add("mysum", DoubleType)
  }

  override def dataType: DataType = StringType


  override def deterministic: Boolean = true

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
    var sampleSqrt: Double = batch.foldLeft(0d) { case (sum, sample) =>
      sum + math.sqrt(sample.asInstanceOf[Double])
    }
    var sampleSumSqrt: Double = math.sqrt(batch.foldLeft(0d) { case (sum, sample) =>
      sum + sample.asInstanceOf[Double]
    } / batch.length)

    var curBatchVar: Double = math.sqrt(tableSizeSqrt) * (sampleSqrt - sampleSumSqrt)

    return curBatchVar
  }


  override def initialize(buffer: MutableAggregationBuffer): Unit = {

    buffer.update(0, 0L)
    buffer.update(1, 0d)
  }


  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, buffer.getAs[Long](0) + 1)

    buffer.update(1, buffer.getAs[Double](1) + input.getAs[Double](0))

    crtCount = buffer.getAs[Long](0)

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
    val sum = buffer.getAs[Double](1)

    updateHistorical()

    val T_n_2 = historicalVar
    var localErrorBound: Double = errorBound
    var localConfidence: Double = confidence

    val updateConfidence = if (confidence == -1) true else false

    if (updateConfidence) {
      localErrorBound = errorBound
      localConfidence = commonMath.calcConfidence(localErrorBound, crtCount, T_n_2)
    } else {
      localConfidence = confidence
      localErrorBound = commonMath.calcErrorBound(localConfidence, crtCount, T_n_2)
    }

    s"runningResult=$sum\tP=$localConfidence\terrorBound=$localErrorBound".toString
  }


}

class OnlineCount(confidence: Double, errorBound: Double, size: Long, fraction: Double)

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

class OnlineMin(confidence: Double, errorBound: Double, size: Long, fraction: Double)
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

class OnlineMax(confidence: Double, errorBound: Double, size: Long, fraction: Double)
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

class OnlineAvg(confidence: Double, errorBound: Double, size: Long)
  extends UserDefinedAggregateFunction with Logging {

  // Input Data Type Schema.
  // Assuming aggregate on single column, and its type is DoubleType.
  override def inputSchema: StructType = {
    new StructType().add("execColumn", DoubleType)
  }

  // Intermediate Schema
  override def bufferSchema: StructType = {
    new StructType()
      .add("count", LongType) // need update
      .add("sum", DoubleType) // need update
      .add("histVar", DoubleType) // need update
      .add("batchSize", IntegerType) // DO NOT need update
      .add("null", DoubleType) // DO NOT need update
      .add("batchPivot", IntegerType) // need update
      .add("histAvg", DoubleType) // need update
      .add("batch_0", DoubleType) // need update
      .add("batch_1", DoubleType) // need update
  }

  // Return type
  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  def updateHistorical(buffer: MutableAggregationBuffer): Unit = {
    // get current avg from buffer
    val crtAvg = buffer.getAs[Double](1) / buffer.getAs[Long](0)
    // get batch from buffer
    var batch = new Array[Double](2)
    batch(0) = buffer.getAs[Double](7)
    batch(1) = buffer.getAs[Double](8)
    var sum = 0d
    val actualLen = batch.size
    for (index <- 0 to (actualLen - 1)) {
      sum += batch(index)
    }
    val batchAvg: Double = sum / actualLen
    val batchVar: Double = calcBatchVar(buffer)

    val historicalCount = buffer.getAs[Long](0) - actualLen
    var historicalVar = buffer.getAs[Double](2)
    var historicalAvg = buffer.getAs[Double](6)

    historicalVar = if (historicalCount == 0) batchVar
    else (
      historicalCount * (historicalVar + math.pow(crtAvg - historicalAvg, 2.0)) +
        buffer.getAs[Int](3) * (batchVar + math.pow(crtAvg - batchAvg, 2.0))
      ) / (historicalCount + buffer.getAs[Int](3))

    historicalAvg = if (historicalCount == 0) batchAvg
    else crtAvg

    buffer.update(2, historicalVar)
    buffer.update(6, historicalAvg)
  }

  def getActualLen(array: GenericArrayData): Int = {
    var loop = new Breaks
    var retVal = array.numElements() - 1
    loop.breakable {
      for (index <- 0 to (array.numElements() - 1)) {
        if (array.array(index) == Double.MinValue) {
          retVal = index
          loop.break()
        }
      }
    }
    retVal
  }

  def calcBatchVar(buffer: MutableAggregationBuffer): Double = {
    var batch = new Array[Double](2)
    batch(0) = buffer.getAs[Double](7)
    batch(1) = buffer.getAs[Double](8)
    var sum = 0d
    val actualLen = batch.size
    for (index <- 0 to (actualLen - 1)) {
      sum += batch(index)
    }

    val batchAvg: Double = sum / actualLen

    var squareSum = 0d
    for (index <- 0 to (actualLen - 1)) {
      squareSum += (batch(index) - batchAvg) * (batch(index) - batchAvg)
    }
    squareSum / actualLen
  }

  // Initialize the Intermediate buffer
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, 0L) // count
    buffer.update(1, 0d) // sum
    buffer.update(2, 0d) // histVar
    buffer.update(3, 2) // batchSize


    buffer.update(4, 0d) // null, just update once!
    buffer.update(5, 0) // batchPivot
    buffer.update(6, 0d) // histAvg
    buffer.update(7, 0d) // batch_0
    buffer.update(8, 0d) // batch_1
  }


  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, buffer.getAs[Long](0) + 1)
    buffer.update(1, buffer.getAs[Double](1) + input.getAs[Double](0))

    var batchPivot = buffer.getAs[Int](5)
    var batchSize = buffer.getAs[Int](3)

    var batch_0 = buffer.getAs[Double](7)
    var batch_1 = buffer.getAs[Double](8)

    if (batchPivot < batchSize) {
      // batch.array(batchPivot) = input.getAs[Double](0)
      if (batchPivot == 0) {
        batch_0 = input.getAs[Double](0)
      } else {
        batch_1 = input.getAs[Double](0)
      }
      batchPivot += 1

      // update batch and pivot
      buffer.update(7, batch_0)
      buffer.update(8, batch_1)
      buffer.update(5, batchPivot)
    } else {
      updateHistorical(buffer)

      // clear the batch
      batch_0 = 0d
      batch_1 = 0d
      batchPivot = 0
      // update batch and pivot
      buffer.update(7, batch_0)
      buffer.update(8, batch_1)
      buffer.update(5, batchPivot)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.getAs[Long](0) + buffer2.getAs[Long](0))
    buffer1.update(1, buffer1.getAs[Double](1) + buffer2.getAs[Double](1))
    var buffer1_avg = buffer1.getAs[Double](1) / buffer1.getAs[Long](0)
    var buffer2_avg = buffer2.getAs[Double](1) / buffer2.getAs[Long](0)
    var buffer1_var = buffer1.getAs[Double](2)
    var buffer2_var = buffer2.getAs[Double](2)
    var total_avg = (buffer1.getAs[Double](1) + buffer2.getAs[Double](1)) /
      (buffer1.getAs[Long](0) + buffer2.getAs[Long](0))

    var new_var = (
      buffer1.getAs[Long](0) * (buffer1_var + math.pow(total_avg - buffer1_avg, 2.0)) +
        buffer2.getAs[Long](0) * (buffer2_var + math.pow(total_avg - buffer2_avg, 2.0))
      ) / (buffer1.getAs[Long](0) + buffer2.getAs[Long](0))
    buffer1.update(2, new_var)
  }

  override def evaluate(buffer: Row): Any = {
    val avg = buffer.getAs[Double](1) / buffer.getAs[Long](0)

    // updateHistorical(buffer)

    var T_n_2 = buffer.getAs[Double](2)
    var localErrorBound: Double = 0d
    var localConfidence = 0d

    val updateConfidence = if (confidence == -1) true else false

    if (updateConfidence) {
      localErrorBound = errorBound
      logError(s"localErrorBound is $localErrorBound, " +
        s"crtCount is ${buffer.getAs[Long](0)}, T_n_2 is $T_n_2")
      localConfidence = commonMath.calcConfidence(localErrorBound, buffer.getAs[Long](0), T_n_2)
    } else {
      localConfidence = confidence
      logError(s"localConfidence is $localConfidence, " +
        s"crtCount is ${buffer.getAs[Long](0)}, T_n_2 is $T_n_2")
      localErrorBound = commonMath.calcErrorBound(localConfidence, buffer.getAs[Long](0), T_n_2)
    }
    logError(s"avg is $avg")
    logError(s"localConfidence is $localConfidence")
    logError(s"errorBound is $localErrorBound")

    s"runningResult=$avg\tP=$localConfidence\terrorBound=$localErrorBound".toString
  }
}

class OnlineStdvar(confidence: Double, errorBound: Double, size: Long, fraction: Double)
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

class OnlineVar(confidence: Double, errorBound: Double, size: Long, fraction: Double)
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

