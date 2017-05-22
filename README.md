# StudySpark
spark的一个小项目

## 2017-05-21 spark sql数据倾斜的学习

### 1. 聚合源数据和spark core没有区别

* 对每个key聚合出一条数据，比如拼接，这些大部分是在hive中完成的
* 如果无法对每个key都聚合出一条数据，就放粗粒度，尽量减少每个key对应的数据量```select ... from ... group by ... ```

### 2. 过滤掉会导致数据倾斜的key

* 如果那个导致数据倾斜的key并不是那么重要的话，就可以把它过滤掉

### 3. 提高shuffle操作的reduce并行度

> 就是将reduce task的数量，变多，就可以让每个reduce task分配到更少的数据量，这样的话，可能就能缓解，甚至基本解决数据倾斜的问题

> **spark.default.parallelism 100** spark shuffle默认并行度是100

具体操作就是，在shuffle算子，比如groupByKey, countByKey, reduceBuKey等的参数中穿进去一个数字，那个数字就是我们指定的并行度

这其实是一个治标不治本的办法，因为它没有从根本上消除数据倾斜，只能缓解和减轻了shuffle reduce task的数据压力。

### 4. 随机key实现双重聚合




## 2017-05-19 得到每个区域top3的产品, 实现自定义的udf和udaf聚合函数

## 2017-05-18 添加页面切片转化率的模块

## 2017-05-17 spark性能优化和数据倾斜的学习

## 2017-05-15 将fastutilDateHourExtractMap转为fastutil，并设为广播变量，并使用Kryo序列化将CategorySortKey序列化

## 2017-05-12 实现得到top10的category和session, 并持久化到数据库

## 2017-05-11 实现按比例随机抽取session，并持久化到数据库

## 2017-05-10 实现自定义的AccumulatorV2，并将session聚合统计持久化到数据库中

## 2017-05-09 实现模拟数据方法MockData方法以及对session的筛选聚合处理
