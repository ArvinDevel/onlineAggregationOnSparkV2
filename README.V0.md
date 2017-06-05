# OnlineAggregationOnSpark

Spark is a fast and general cluster computing system for Big Data. It provides
high-level APIs in Scala, Java, Python, and R, and an optimized engine that
supports general computation graphs for data analysis. It also supports a
rich set of higher-level tools including Spark SQL for SQL and DataFrames,
MLlib for machine learning, GraphX for graph processing,
and Spark Streaming for stream processing.

我们在Spark上通过修改聚合算子(么司机补充)，实现了常见聚合函数(AVG, SUM, COUNT, MIN, MAX, STDVAR, VAR)的Online版本。

<https://github.com/ArvinDevel/onlineAggregationOnSparkV2>


## Install

项目使用 Maven 构建，To build our project, just run:
    
    build/mvn -DskipTests clean package

## Running Tests

The easiest way to start using Spark is through the Scala shell:

    ./bin/spark-shell

Try the following command:

    val df =  sqlContext.read.json("testdata/time-series-data-for-major-economic-and-demographic-variables.json")

    在这里写几个运行命令

### 运行API：

函数

`onlineAggregate(aggregateFuncName: String, aggregateField: String, groupByCol1: String,groupByCols: String*)`

作为`DataFrame`对象的`OnlineAggregation`的入口，第一个参数为聚集操作名称，包括平均，求和，计数，最小及最大（`avg`，`count`，`min`，`max`，`sum`）等，第二个参数为聚集操作针对的字段，后面的操作为分组字段，可以接受多个参数。

### 运行示例：

`df.onlineAggregate("avg", "age", "name")`

表示对`df`这个`DataFrame`对象调用`OnlineAggregation`，其中聚集操作为`average`，针对的字段是`“age”`，而`groupby`的字段是`“name”`
