/*
 *
 */

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

class MyAvg extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = {
    new StructType().add("myinput", DoubleType)
  }

  override def bufferSchema: StructType = {
    new StructType().add("mycnt", LongType).add("mysum", DoubleType)
  }


  override def dataType: DataType = DoubleType


  override def deterministic: Boolean = true


  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, 0l)
    buffer.update(1, 0d)
  }

  //partitions内部combine
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, buffer.getAs[Long](0) + 1) //条数加1
    buffer.update(1, buffer.getAs[Double](1) + input.getAs[Double](0)) //输入汇总
    //目前1.5版本好像还有点问题，不能通过字段名来取值
    //      buffer.update(0, buffer.getAs[Long]("mycnt") + 1) //条数加1
    //      buffer.update(1, buffer.getAs[Double]("mysum") + input.getAs[Double]("myinput")) //输入汇总
  }

  //MutableAggregationBuffer继承自Row
  //partitions间合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.getAs[Long](0) + buffer2.getAs[Long](0))
    buffer1.update(1, buffer1.getAs[Double](1) + buffer2.getAs[Double](1))
    //目前1.5版本好像还有点问题，不能通过字段名来取值
    //      buffer1.update(0, buffer1.getAs[Long]("mycnt") + buffer2.getAs[Long]("mycnt"))
    //      buffer1.update(1, buffer1.getAs[Double]("mysum") + buffer2.getAs[Double]("mysum"))
  }


  override def evaluate(buffer: Row): Any = {
    //计算平均值
    val avg = buffer.getAs[Double](1) / buffer.getAs[Long](0)
    //目前1.5版本好像还有点问题，不能通过字段名来取值
    //      val avg = buffer.getAs[Double]("mysum") / buffer.getAs[Long]("mycnt")
    f"$avg%.2f".toDouble
  }


}