# OnlineAggregationOnSpark

## Introduction
Spark is a fast and general cluster computing system for Big Data. It provides
high-level APIs in Scala, Java, Python, and R, and an optimized engine that
supports general computation graphs for data analysis. It also supports a
rich set of higher-level tools including Spark SQL for SQL and DataFrames,
MLlib for machine learning, GraphX for graph processing,
and Spark Streaming for stream processing.
Spark 是由 UC Berkeley 的 AMPLab 主导开发的基于内存计算的大数据处理框架，
Spark SQL 作为其重要的组成部分，用于处理结构化数据

我们在Spark源码(`V1.5.1`)上通过修改聚合算子，实现了常见聚合函数(AVG, SUM, COUNT, MIN, MAX, STDVAR, VAR)的Online版本。


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

该函数是`DataFrame`对象的方法，用户通过该函数设置终止的置信度和置信区间。

#### 函数 onlineAggregate

    def onlineAggregate(aggregateFuncName: String, aggregateField: String, groupByCol1: String,groupByCols: String*): Unit

该函数作为`DataFrame`对象的`OnlineAggregation`的入口，第一个参数为聚集操作名称，包括平均，求和，计数，最小及最大（`avg`，`count`，`min`，`max`，`sum`）等，第二个参数为聚集操作针对的字段，后面的操作为分组字段，可以接受多个参数。

### 运行示例：

测试数据在项目根目录`testdata`下，共有三份测试数据:

-   `testdata/employees.json`: 144条记录
-   `testdata/people.json`: 126条记录
-   `testdata/thads2013n.json`: 64535条记录


    scala> val df =  sqlContext.read.json("testdata/thads2013n.json")

    scala> df.setTermination(0.99,0.01)

    scala> df.onlineAggregate("avg", "L30", "ME")

表示对`df`这个`DataFrame`对象调用`OnlineAggregation`，其中聚集操作为`average`，针对的字段是`“L30”`，而`groupby`的字段是`“ME”`

返回结果如下：

    sample percentage: 0.21000000000000002
    ['2',R=19025.303135888502	P=0.95	E=90.7413816246117]
    ['3',R=17494.54676258993	P=0.95	E=133.33365869228615]
    ['4',R=14347.854855923159	P=0.95	E=126.90695515940877]
    ['5',R=14258.597709377236	P=0.95	E=102.78623383577974]
    ['1',R=18033.55144404332	P=0.95	E=91.85072892606517]
    ...
    sample percentage: 0.41000000000000003
    ['2',R=19074.593118996276	P=0.95	E=63.58036975492021]
    ['3',R=17475.7177589852     P=0.95	E=92.82041219328019]
    ['4',R=14248.431202600217	P=0.95	E=89.13609756304453]
    ['5',R=14213.03611898017	P=0.95	E=68.83323159705088]
    ['1',R=18129.572733500798	P=0.95	E=64.49776580974893]
    ...
    




