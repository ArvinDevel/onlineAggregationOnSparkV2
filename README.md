# OnlineAggregationOnSpark

## Introduction
Spark is a fast and general cluster computing system for Big Data. It provides
high-level APIs in Scala, Java, Python, and R, and an optimized engine that
supports general computation graphs for data analysis. It also supports a
rich set of higher-level tools including Spark SQL for SQL and DataFrames,
MLlib for machine learning, GraphX for graph processing,
and Spark Streaming for stream processing.(换成中文)

我们在Spark源码(`V1.5.1`)上通过修改聚合算子(么司机补充)，实现了常见聚合函数(AVG, SUM, COUNT, MIN, MAX, STDVAR, VAR)的Online版本。


### 对源码所修改的模块

- `<module>sql/core</module>`中修改了`DataFrame`和`GroupedData`的实现。
- `<module>sql/core</module>`中新增了`OnlineAggregates.scala`文件，在其中实现了各个`OnlineAggregation`算子:
  - OnlineAvg
  - OnlineCount
  - OnlineSum
  - OnlineMin
  - OnlineMax

### 项目`GitHub`地址

<https://github.com/ArvinDevel/onlineAggregationOnSparkV2>


## Install

项目使用 Maven 构建，To build our project, just run:
    
    build/mvn -DskipTests clean package

## Running

编译成功后，在项目的根路径执行:

    ./bin/spark-shell

进入`scala shell`交互执行界面。

### 运行API：

#### 函数 setTermination

    def setTermination(end_confidence: Double, end_errorBound: Double): Unit

该函数是`DataFrame`对象的方法，用户通过该函数设置终止的置信度和置信区间。

#### 函数 onlineAggregate

    def onlineAggregate(aggregateFuncName: String, aggregateField: String, groupByCol1: String,groupByCols: String*): Unit

该函数作为`DataFrame`对象的`OnlineAggregation`的入口，第一个参数为聚集操作名称，包括平均，求和，计数，最小及最大（`avg`，`count`，`min`，`max`，`sum`）等，第二个参数为聚集操作针对的字段，后面的操作为分组字段，可以接受多个参数。

### 运行示例：

    scala> val df =  sqlContext.read.json("testdata/thads2013n.json")

    scala> df.setTermination(0.99,0.01)

    scala> df.onlineAggregate("avg", "age", "name")

表示对`df`这个`DataFrame`对象调用`OnlineAggregation`，其中聚集操作为`average`，针对的字段是`“age”`，而`groupby`的字段是`“name”`

返回结果如下：


