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
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

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

  private var square_sum : Double = 0d
  private var sum_square : Double = 0d
  private var probility : Double = 0d
  private var interval : Double = 0d
  private var deta : Double = 0d

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, buffer.getAs[Long](0) + 1)

    buffer.update(1, buffer.getAs[Double](1) + 1)

    sum_square = buffer.getAs[Long](0)
    square_sum = square_sum + 1L

  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.getAs[Long](0) + buffer2.getAs[Long](0))
    buffer1.update(1, buffer1.getAs[Double](1) + buffer2.getAs[Double](1))

  }

  override def evaluate(buffer: Row): Any = {
    deta = size * size * square_sum -
      size * size * sum_square *sum_square / size / fraction / size / fraction
    val updateConfidence = if (confidence == -1) true else false

    if (updateConfidence) {
      interval = errorBound
      probility = interval * math.pow(size * fraction, 1/2) / math.pow(deta, 1/2) * 2 -1
    } else {
      probility = confidence
      interval = (1 + probility) / 2 * math.pow(math.pow(deta, 1/2) / (size * fraction), 1/2)
    }

    s"${ buffer.getAs[Long](0)/fraction}%.2f\tP=$probility\terrorBound=$interval".toString
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

  private var confidence_1 = 0.9d

  private var sampleCount = fraction * size // the count of samples
  private var filterFraction = 1d // filter_sample_count / sample_count

  private var min = 99999999d

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

    if (min >= input.getAs[Double](0)){
      min = input.getAs[Double](0)
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

    var localErrorBound1: Double = 0d
    var localErrorBound2: Double = 0d
    var quantile = 0d

    val Z_P = normalInv((1 + confidence_1) / 2)
    val T_N_U = crtCount / sampleCount
    val Z_N_S = historicalVar
    val R_N_2 = crtSum / crtCount
    val R_N_1 = crtSum_2 / crtCount
    val T_N_2_U = (crtCount*(1-filterFraction) +
      (0-filterFraction)*sampleCount*(1-filterFraction))/crtCount*filterFraction

    T_N_1_1_UV2_U = (1 - filterFraction) * ( crtSum_2 - crtSum_2/crtCount*filterFraction*crtCount) +
      (0-filterFraction)*((0-crtSum_2)/crtCount*filterFraction)*(sampleCount*(1-filterFraction))

    T_N_1_1_UV_U = (1 - filterFraction) * ( crtSum - crtSum/crtCount*filterFraction*crtCount) +
      (0-filterFraction)*((0-crtSum)/crtCount*filterFraction)*(sampleCount*(1-filterFraction))

    T_N_1_1_UV2_U = T_N_1_1_UV2_U/crtCount*filterFraction
    T_N_1_1_UV_U = T_N_1_1_UV_U/crtCount*filterFraction

    val G_N = historicalVar_2 - 4*R_N_2 * T_N_1_1_UV2_UV +
      (4*R_N_2*R_N_2 - 2*R_N_1)*T_N_1_1_UV2_U +
      4*R_N_2*R_N_2*historicalVar_3 +
      (4*R_N_1*R_N_2-8*R_N_2*R_N_2*R_N_2)*T_N_1_1_UV_U +
      math.pow((2*R_N_2*R_N_2-R_N_1), 2)*T_N_2_U

    localErrorBound1 = math.pow(Z_P*Z_P*historicalVar/crtCount, 1/2)

    localErrorBound2 =
      math.pow(Z_P*Z_P*G_N/crtCount*filterFraction/filterFraction/filterFraction, 1/2)

    quantile = (historicalVar + localErrorBound2)/
      (historicalVar + localErrorBound2 + math.pow(min - historicalAvg-localErrorBound1, 2))

    s"$min\tP=$quantile\t$localErrorBound1".toString
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

  private var confidence_1 = 0.9d

  private var sampleCount = fraction * size // the count of samples
  private var filterFraction = 1d // filter_sample_count / sample_count

  private var max = -999999d

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

    if (max <= input.getAs[Double](0)){
      max = input.getAs[Double](0)
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

    var localErrorBound1: Double = 0d
    var localErrorBound2: Double = 0d
    var quantile = 0d

    val Z_P = normalInv((1 + confidence_1) / 2)
    val T_N_U = crtCount / sampleCount
    val Z_N_S = historicalVar
    val R_N_2 = crtSum / crtCount
    val R_N_1 = crtSum_2 / crtCount
    val T_N_2_U = (crtCount*(1-filterFraction) +
      (0-filterFraction)*sampleCount*(1-filterFraction))/crtCount*filterFraction

    T_N_1_1_UV2_U = (1 - filterFraction) * ( crtSum_2 - crtSum_2/crtCount*filterFraction*crtCount) +
      (0-filterFraction)*((0-crtSum_2)/crtCount*filterFraction)*(sampleCount*(1-filterFraction))

    T_N_1_1_UV_U = (1 - filterFraction) * ( crtSum - crtSum/crtCount*filterFraction*crtCount) +
      (0-filterFraction)*((0-crtSum)/crtCount*filterFraction)*(sampleCount*(1-filterFraction))

    T_N_1_1_UV2_U = T_N_1_1_UV2_U/crtCount*filterFraction
    T_N_1_1_UV_U = T_N_1_1_UV_U/crtCount*filterFraction

    val G_N = historicalVar_2 - 4*R_N_2 * T_N_1_1_UV2_UV +
      (4*R_N_2*R_N_2 - 2*R_N_1)*T_N_1_1_UV2_U +
      4*R_N_2*R_N_2*historicalVar_3 +
      (4*R_N_1*R_N_2-8*R_N_2*R_N_2*R_N_2)*T_N_1_1_UV_U +
      math.pow((2*R_N_2*R_N_2-R_N_1), 2)*T_N_2_U

    localErrorBound1 = math.pow(Z_P*Z_P*historicalVar/crtCount, 1/2)

    localErrorBound2 =
      math.pow(Z_P*Z_P*G_N/crtCount*filterFraction/filterFraction/filterFraction, 1/2)

    quantile = 1 - (historicalVar + localErrorBound2)/
      (historicalVar + localErrorBound2 + math.pow(max - historicalAvg-localErrorBound1, 2))

    s"$max\tP=$quantile\t$localErrorBound1".toString
  }

}

class OnlineAvg(confidence: Double, errorBound: Double, size: Long)
  extends UserDefinedAggregateFunction {

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
    else crtAvg
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

    var T_n_2 = historicalVar
    var localErrorBound: Double = 0d
    var localConfidence = 0d

    val updateConfidence = if (confidence == -1) true else false

    if (updateConfidence) {
      localErrorBound = errorBound
      localConfidence = commonMath.calcConfidence(localErrorBound, crtCount, T_n_2)
    } else {
      localConfidence = confidence
      localErrorBound = commonMath.calcErrorBound(localConfidence, crtCount, T_n_2)
    }

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
    val T_N_2_U = (crtCount*(1-filterFraction) +
      (0-filterFraction)*sampleCount*(1-filterFraction))/crtCount*filterFraction

    T_N_1_1_UV2_U = (1 - filterFraction) * ( crtSum_2 - crtSum_2/crtCount*filterFraction*crtCount) +
      (0-filterFraction)*((0-crtSum_2)/crtCount*filterFraction)*(sampleCount*(1-filterFraction))

    T_N_1_1_UV_U = (1 - filterFraction) * ( crtSum - crtSum/crtCount*filterFraction*crtCount) +
      (0-filterFraction)*((0-crtSum)/crtCount*filterFraction)*(sampleCount*(1-filterFraction))

    T_N_1_1_UV2_U = T_N_1_1_UV2_U/crtCount*filterFraction
    T_N_1_1_UV_U = T_N_1_1_UV_U/crtCount*filterFraction

    val G_N = historicalVar_2 - 4*R_N_2 * T_N_1_1_UV2_UV +
      (4*R_N_2*R_N_2 - 2*R_N_1)*T_N_1_1_UV2_U +
      4*R_N_2*R_N_2*historicalVar_3 +
      (4*R_N_1*R_N_2-8*R_N_2*R_N_2*R_N_2)*T_N_1_1_UV_U +
      math.pow((2*R_N_2*R_N_2-R_N_1), 2)*T_N_2_U

    val updateConfidence = if (confidence == -1) true else false

    if (updateConfidence) {
      localErrorBound2 = errorBound
      confidence_1 = CNDF(
        math.pow(localErrorBound2*crtCount/filterFraction*filterFraction*filterFraction/G_N, 1/2))
    } else {
      confidence_1 = confidence
      localErrorBound2 =
        math.pow(Z_P*Z_P*G_N/crtCount*filterFraction/filterFraction/filterFraction, 1/2)
    }

    s"$historicalVar%.2f\tP=$confidence_1\terrorBound=$localErrorBound2".toString
  }
}

