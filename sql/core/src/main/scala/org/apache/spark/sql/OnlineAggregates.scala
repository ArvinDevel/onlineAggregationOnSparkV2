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

import org.apache.spark.sql.commonMath.normalInv
import org.apache.spark.sql.commonMath.CNDF
import scala.util.control._
import org.apache.spark.Logging
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


import scala.collection.mutable
import scala.collection.mutable.ListBuffer

// every impl class should use confidence and errorBound as parameter


class OnlineSum(confidence: Double, errorBound: Double, size: Long)
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

  def calcBatchVar(buffer: MutableAggregationBuffer): Double = {
    val tablesize: Double = size
    var columnSqrt: Double = 0d
    var columnSumSqrt: Double = 0d
    var batch = new Array[Double](2)
    batch(0) = buffer.getAs[Double](7)
    batch(1) = buffer.getAs[Double](8)
    var sum = 0d
    val actualLen = batch.size
    for (index <- 0 to (actualLen - 1)) {
      sum += batch(index)
    }

    for (index <- 0 to (actualLen - 1)) {
      columnSqrt += math.sqrt(batch(index))
    }

    columnSumSqrt = math.sqrt(sum)

    math.sqrt(tablesize) * (columnSqrt - (columnSqrt / math.sqrt(actualLen)))
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
    val sum = buffer.getAs[Double](1)

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
    logError(s"sum is $sum")
    logError(s"localConfidence is $localConfidence")
    logError(s"errorBound is $localErrorBound")

    s"runningResult=$sum\tP=$localConfidence\terrorBound=$localErrorBound".toString
  }

}

class OnlineCount(confidence: Double, errorBound: Double, size: Long, fraction: Double)

  extends UserDefinedAggregateFunction with Logging{
  override def inputSchema: StructType = {
    new StructType().add("myinput", DoubleType)
  }

  override def bufferSchema: StructType = {
    new StructType()
      .add("mycnt", LongType)
      .add("mysum", DoubleType)
      .add("square_sum", DoubleType)
      .add("sum_square", DoubleType)
  }

  override def dataType: DataType = StringType


  override def deterministic: Boolean = true


  override def initialize(buffer: MutableAggregationBuffer): Unit = {

    buffer.update(0, 0L)
    buffer.update(1, 0d)
    buffer.update(2, 0d)
    buffer.update(3, 0d)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, buffer.getAs[Long](0) + 1)
    buffer.update(1, buffer.getAs[Double](1) + 1)
    buffer.update(2, buffer.getAs[Double](2) + 1d)
    buffer.update(3, buffer.getAs[Double](3) + 1d)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.getAs[Long](0) + buffer2.getAs[Long](0))
    buffer1.update(1, buffer1.getAs[Double](1) + buffer2.getAs[Double](1))
    buffer1.update(2, buffer1.getAs[Double](2) + buffer2.getAs[Double](2))
    buffer1.update(3, buffer1.getAs[Double](3) + buffer2.getAs[Double](3))
  }

  override def evaluate(buffer: Row): Any = {
    var probility: Double = 0d
    var interval: Double = 0d
    var deta: Double = 0d

    var square_sum = buffer.getAs[Double](2)
    var sum_square = buffer.getAs[Double](3)

    deta = size * size * square_sum -
      size * size * sum_square * sum_square / size / fraction / size / fraction
    val updateConfidence = if (confidence == -1) true else false

    if (updateConfidence) {
      interval = errorBound
      probility = 2 * CNDF((interval * math.sqrt(size * fraction)) / math.sqrt(deta)) - 1
    } else {
      probility = confidence
      var z_p = normalInv((1 + probility) / 2)
      interval = z_p * math.sqrt(deta / (size * fraction))
    }

    var buffer0 = buffer.getAs[Long](0)
    var buffer1 = buffer.getAs[Long](1)
    var buffer2 = buffer.getAs[Long](2)
    var buffer3 = buffer.getAs[Long](3)

    logError(s"count is $buffer0")
    logError(s"sum is $buffer1")
    logError(s"square_sum is $buffer2")
    logError(s"sum_square is $buffer3")

    logError(s"fraction is $fraction")
    logError(s"size is $size")
    logError(s"count in buffer is ${buffer.getAs[Long](0)}")

    s"${buffer.getAs[Long](0) / fraction}\tP=$probility\terrorBound=$interval".toString
  }
}

class OnlineMin(confidence: Double, errorBound: Double, size: Long, fraction: Double)
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
      .add("min", DoubleType)
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
    buffer.update(9, 999999d) // min
  }


  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, buffer.getAs[Long](0) + 1)
    buffer.update(1, buffer.getAs[Double](1) + input.getAs[Double](0))

    var min = buffer.getAs[Double](9)
    if (min > input.getAs[Double](0)) {
      buffer.update(9, input.getAs[Double](0))
    }

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
    val min = buffer.getAs[Double](9)

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
    logError(s"min is $min")
    logError(s"localConfidence is $localConfidence")
    logError(s"errorBound is $localErrorBound")

    s"runningResult=$min\tP=$localConfidence\terrorBound=$localErrorBound".toString
  }
}

class OnlineMax(confidence: Double, errorBound: Double, size: Long, fraction: Double)
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
      .add("max", DoubleType) // max
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
    buffer.update(9, 0d) // max
  }


  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, buffer.getAs[Long](0) + 1)
    buffer.update(1, buffer.getAs[Double](1) + input.getAs[Double](0))

    var max = buffer.getAs[Double](9)

    if (max < input.getAs[Double](0)) {
      buffer.update(9, input.getAs[Double](0))
    }

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
    val max = buffer.getAs[Double](9)

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
    logError(s"max is $max")
    logError(s"localConfidence is $localConfidence")
    logError(s"errorBound is $localErrorBound")

    s"runningResult=$max\tP=$localConfidence\terrorBound=$localErrorBound".toString
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

  private val batchSize = 10000
  private var batch = new ListBuffer[Double]()
  private var batchPivot = 0
  private var historicalAvg = 0d
  private var historicalVar = 0d

  private var crtCount = 0L
  private var crtSum = 0d
  private var crtSum_2 = 0d

  private val batchSize_2 = 10000
  private var batch_2 = new ListBuffer[Double]()
  private var batchPivot_2 = 0
  private var historicalAvg_2 = 0d
  private var historicalVar_2 = 0d

  private var historicalAvg_3 = 0d
  private var historicalVar_3 = 0d

  private var T_N_1_1_UV2_UV = 0d
  private var T_N_1_1_UV2_U = 0d
  private var T_N_1_1_UV_U = 0d

  private var confidence_1 = 0d

  private var sampleCount = fraction * size // the count of samples
  private var filterFraction = 1d // filter_sample_count / sample_count

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
    else crtAvg
  }

  def calcBatchVar(): Double = {
    val batchAvg: Double = batch.sum / batch.length
    batch.foldLeft(0d) { case (sum, sample) =>
      sum + (sample - batchAvg) * (sample - batchAvg)
    } / batch.length
  }

  def updateHistorical3(): Unit = {
    val crtAvg = crtSum / crtCount * filterFraction

    val batchAvg: Double = batch.sum / batch.length * filterFraction
    val batchVar: Double = calcBatchVar3()
    val historicalCount = crtCount - batch.length

    historicalVar_3 = if (historicalCount == 0) batchVar
    else (
      historicalCount / filterFraction * (historicalVar_3 +
        math.pow(crtAvg - historicalAvg_3, 2.0)) +
        batchSize / filterFraction * (batchVar + math.pow(crtAvg - batchAvg, 2.0))
      ) / (historicalCount / filterFraction + batchSize / filterFraction)

    historicalAvg_3 = if (historicalCount == 0) batchAvg
    else crtAvg
  }

  def calcBatchVar3(): Double = {
    val batchAvg: Double = batch.sum / batch.length * filterFraction
    batch.foldLeft(0d) { case (sum, sample) =>
      sum + (sample - batchAvg) * (sample - batchAvg)
    } / batch.length * filterFraction
  }

  def updateHistorical2(): Unit = {
    val crtAvg = crtSum_2 / crtCount * filterFraction

    val batchAvg: Double = batch_2.sum / batch_2.length * filterFraction
    val batchVar: Double = calcBatchVar2()
    val historicalCount = crtCount - batch_2.length

    historicalVar_2 = if (historicalCount == 0) batchVar
    else (
      historicalCount / filterFraction *
        (historicalVar_2 + math.pow(crtAvg - historicalAvg_2, 2.0))
        + batchSize / filterFraction * (batchVar + math.pow(crtAvg - batchAvg, 2.0))
      ) / (historicalCount / filterFraction + batchSize / filterFraction)

    historicalAvg_2 = if (historicalCount == 0) batchAvg
    else crtAvg
  }

  def calcBatchVar2(): Double = {
    val batchAvg: Double = batch_2.sum / batch_2.length * filterFraction
    batch_2.foldLeft(0d) { case (sum, sample) =>
      sum + (sample - batchAvg) * (sample - batchAvg)
    } / batch_2.length * filterFraction
  }

  override def dataType: DataType = StringType


  override def deterministic: Boolean = true


  override def initialize(buffer: MutableAggregationBuffer): Unit = {

    buffer.update(0, 0L)
    buffer.update(1, 0d)
  }

  def calc(): Double = {
    val batchAvg: Double = batch.sum / batch.length * filterFraction
    val batchAvg1: Double = batch_2.sum / batch_2.length * filterFraction

    var sum : Double = 0d
    var i : Int = 0
    while (i < batch.length){
      sum + (batch(i) - batchAvg) * (batch_2(i) - batchAvg1)
    }
    i = i + 1
    sum
  }

  def update_T_n_n() : Unit = {
    val crtAvg = crtSum / crtCount * filterFraction
    val crtAvg1 = crtSum_2 / crtCount * filterFraction

    T_N_1_1_UV2_UV = T_N_1_1_UV2_UV + calc()

  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, buffer.getAs[Long](0) + 1)
    buffer.update(1, buffer.getAs[Double](1) + input.getAs[Double](0))

    crtCount = buffer.getAs[Long](0)
    crtSum = buffer.getAs[Double](1)
    crtSum_2 = crtSum_2 + input.getAs[Double](0) * input.getAs[Double](0)

    if (batchPivot < batchSize) {
      batch += input.getAs[Double](0)
      batch_2 += input.getAs[Double](0)
      batchPivot += 1
      batchPivot_2 += 1
    } else {
      updateHistorical()
      updateHistorical2()
      updateHistorical3()
      update_T_n_n()
      batch.clear()
      batch_2.clear()
      batchPivot = 0
      batchPivot_2 = 0
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.getAs[Long](0) + buffer2.getAs[Long](0))
    buffer1.update(1, buffer1.getAs[Double](1) + buffer2.getAs[Double](1))

  }

  override def evaluate(buffer: Row): Any = {
    updateHistorical()
    updateHistorical2()
    updateHistorical3()
    update_T_n_n()

    var localErrorBound2: Double = 0d

    val Z_P = normalInv((1 + confidence) / 2)
    val T_N_U = crtCount / sampleCount
    val Z_N_S = historicalVar
    val R_N_2 = crtSum / crtCount
    val R_N_1 = crtSum_2 / crtCount
    val T_N_2_U = (crtCount * (1 - filterFraction) +
      (0 - filterFraction) * sampleCount * (1 - filterFraction)) / crtCount * filterFraction

    T_N_1_1_UV2_U =
      (1 - filterFraction) *
        (crtSum_2 - crtSum_2 / crtCount * filterFraction * crtCount) +
        (0 - filterFraction) *
          ((0 - crtSum_2) / crtCount * filterFraction) * (sampleCount * (1 - filterFraction))

    T_N_1_1_UV_U =
      (1 - filterFraction) *
        (crtSum - crtSum / crtCount * filterFraction * crtCount) +
        (0 - filterFraction) *
          ((0 - crtSum) / crtCount * filterFraction) * (sampleCount * (1 - filterFraction))

    T_N_1_1_UV2_U = T_N_1_1_UV2_U / crtCount * filterFraction
    T_N_1_1_UV_U = T_N_1_1_UV_U / crtCount * filterFraction

    val G_N = historicalVar_2 - 4 * R_N_2 * T_N_1_1_UV2_UV +
      (4 * R_N_2 * R_N_2 - 2 * R_N_1) * T_N_1_1_UV2_U +
      4 * R_N_2 * R_N_2 * historicalVar_3 +
      (4 * R_N_1 * R_N_2 - 8 * R_N_2 * R_N_2 * R_N_2) * T_N_1_1_UV_U +
      math.pow((2 * R_N_2 * R_N_2 - R_N_1), 2) * T_N_2_U

    val updateConfidence = if (confidence == -1) true else false

    if (updateConfidence) {
      localErrorBound2 = errorBound
      confidence_1 = CNDF(
        math.pow(localErrorBound2 *
          crtCount / filterFraction * filterFraction * filterFraction / G_N, 1 / 2))
    } else {
      confidence_1 = confidence
      localErrorBound2 =
        math.pow(Z_P * Z_P * G_N
          / crtCount * filterFraction / filterFraction / filterFraction, 1 / 2)
    }

    s"$historicalVar%.2f\tP=$confidence_1\terrorBound=$localErrorBound2".toString
  }
}

