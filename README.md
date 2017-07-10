# StudySpark
spark的一个小项目


## 用户访问session分析模块
用户访问session分析业务，session聚合统计、session随机抽取、top10热门分类、top10活跃用户  
技术点：数据的过滤与聚合、自定义Accumulator、按时间比例随机抽取算法、二次排序、分组取topN  
性能调优方案：普通调优、jvm调优、shuffle调优、算子调优  
troubleshooting经验  
数据倾斜解决方案：7种方案  

### 数据库连接池与JDBC辅助组件
因为与数据的连接(Connection)是一种很重要的资源，如果每次使用数据库都要创建一个连接是很耗费资源的，所以设计一个数据库连接池，
创建一些数据库连接存放在里面，要用的时候从里面取，用完了不是直接关闭而是再放回去，这样就能节省资源。  
主要的代码参见：[JDBCHelper](./src/main/java/com/yt/spark/jdbc/JDBCHelper.java)

### 自定义Accumulator
自定义Accumulator要继承AccumulatorV2
[SessionAggrStatAccumulator](./src/main/java/com/yt/spark/spark/session/SessionAggrStatAccumulator.java)

### 二次排序
二次排序就是要继承Ordered，和java的Comparator很像
[CategorySortKey](./src/main/java/com/yt/spark/spark/session/CategorySortKey.java)

### 性能调优

#### 调节并行度
并行度：其实就是指的是，Spark作业中，各个stage的task数量，也就代表了Spark作业的在各个阶段（stage）的并行度。  
> 假设，现在已经在spark-submit脚本里面，给我们的spark作业分配了足够多的资源，比如50个executor，每个executor有10G内存，每个executor有3个cpu core。基本已经达到了集群或者yarn队列的资源上限。
  但是task没有设置，或者设置的很少，比如就设置了，100个task。50个executor，每个executor有3个cpu core，也就是说，你的Application任何一个stage运行的时候，都有总数在150个cpu core，可以并行运行。但是你现在，只有100个task，平均分配一下，每个executor分配到2个task，ok，那么同时在运行的task，只有100个，每个executor只会并行运行2个task。每个executor剩下的一个cpu core，就浪费掉了。

1. 官方是推荐，task数量，设置成spark application总cpu core数量的2~3倍，比如150个cpu core，基本要设置task数量为300~500

2. 如何设置一个Spark Application的并行度 
``` scala
SparkConf conf = new SparkConf()
  .set("spark.default.parallelism", "500")
```

#### 重构RDD与持久化

1. RDD架构重构与优化
尽量去复用RDD，差不多的RDD，可以抽取称为一个共同的RDD，供后面的RDD计算时，反复使用。

2. 公共RDD一定要实现持久化
对于要多次计算和使用的公共RDD，一定要进行持久化。

3. 持久化，是可以进行序列化的
如果正常将数据持久化在内存中，那么可能会导致内存的占用过大，这样的话，也许，会导致OOM内存溢出。  
当纯内存无法支撑公共RDD数据完全存放的时候，就优先考虑，使用序列化的方式在纯内存中存储。将RDD的每个partition的数据，序列化成一个大的字节数组，就一个对象；序列化后，大大减少内存的空间占用。  
如果序列化纯内存方式，还是导致OOM，内存溢出；就只能考虑磁盘的方式，内存+磁盘的普通方式（无序列化）。  
**缺点**：在获取数据的时候需要反序列化

4. 为了数据的高可靠性，而且内存充足，可以使用双副本机制，进行持久化
在内存资源很充沛的情况下，可以持久化一个副本

#### 广播大变量
而每个task在处理变量的时候，都会拷贝一份变量的副本，如果变量很大的话，就会耗费很多内存。这时可以采用广播变量的方式，把这个变量广播出去，因为广播变量只在每个节点的Executor才一份副本
广播变量在初始的时候，就只在Driver上有一份。task在运行的时候，想要使用广播变量中的数据，此时首先会在自己本地的Executor对应的BlockManager中，尝试获取变量副本；如果本地没有，那么就从Driver远程拉取变量副本，并保存在本地的BlockManager中；此后这个executor上的task，都会直接使用本地的BlockManager中的副本。
executor的BlockManager除了从driver上拉取，也可能从其他节点的BlockManager上拉取变量副本，总之越近越好。

#### 使用Kryo序列化
默认情况下，Spark内部是使用Java的序列化机制，ObjectOutputStream/ObjectInputStream，对象输入输出流机制，来进行序列化。  
**优点**：处理起来比较方便，只是在算子里面使用的变量，必须是实现Serializable接口的。  
**缺点**：默认的序列化机制的效率不高，序列化的速度比较慢；序列化以后的数据，占用的内存空间相对还是比较大。  
Kryo序列化机制，比默认的Java序列化机制，速度要快，序列化后的数据要更小，大概是Java序列化机制的1/10。
Kryo序列化机制，一旦启用以后，会生效的几个地方：
1. 算子函数中使用到的外部变量，使用Kryo后，优化网络传输的性能，可以优化集群中内存的占用和消耗
2. 持久化RDD时进行序列化，StorageLevel.MEMORY_ONLY_SER。持久化RDD占用的内存越少，task执行的时候创建的对象，就不至于频繁的占满内存，频繁发生GC。
3. shuffle：可以优化网络传输的性能

使用Kryo序列化：
1. SparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
2. 注册你使用到的，需要通过Kryo序列化的自定义类。SparkConf.registerKryoClasses(new Class[]{CategorySoryKey.class})

#### 使用fastutil优化数据格式
* fastutil介绍
> fastutil是扩展了Java标准集合框架（Map、List、Set；HashMap、ArrayList、HashSet）的类库，提供了特殊类型的map、set、list和queue；
  fastutil能够提供更小的内存占用，更快的存取速度；我们使用fastutil提供的集合类，来替代自己平时使用的JDK的原生的Map、List、Set，好处在于，fastutil集合类，可以减小内存的占用，并且在进行集合的遍历、根据索引（或者key）获取元素的值和设置元素的值的时候，提供更快的存取速度；
  fastutil也提供了64位的array、set和list，以及高性能快速的，以及实用的IO类，来处理二进制和文本类型的文件
  
fastutil的每一种集合类型，都实现了对应的Java中的标准接口（比如fastutil的map，实现了Java的Map接口），因此可以直接放入已有系统的任何代码中。
fastutil还提供了一些JDK标准类库中没有的额外功能（比如双向迭代器）。

fastutil除了对象和原始类型为元素的集合，fastutil也提供引用类型的支持，但是对引用类型是使用等于号（=）进行比较的，而不是equals()方法。

* Spark中应用fastutil的场景

1. 如果算子函数使用了外部变量；那么第一，你可以使用Broadcast广播变量优化；第二，可以使用Kryo序列化类库，提升序列化性能和效率；第三，如果外部变量是某种比较大的集合，那么可以考虑使用fastutil改写外部变量，首先从源头上就减少内存的占用，通过广播变量进一步减少内存占用，再通过Kryo序列化类库进一步减少内存占用。

2. 在你的算子函数里，如果要创建比较大的Map、List等集合，可能会占用较大的内存空间，而且可能涉及到消耗性能的遍历、存取等集合操作；那么此时，可以考虑将这些集合类型使用fastutil类库重写，使用了fastutil集合类以后，就可以在一定程度上，减少task创建出来的集合类型的内存占用。避免executor内存频繁占满，频繁唤起GC，导致性能下降。

``` xml
<dependency>
  <groupId>it.unimi.dsi</groupId>
  <artifactId>fastutil</artifactId>
  <version>7.0.6</version>
</dependency>
```
> UserVisitSessionAnalyzeSpark.scala中831行有示例。

#### 调节数据本地化等待时长
PROCESS_LOCAL > NODE_LOCAL > NO_PREF > RACK_LOCAL > ANY  
Spark要对任务(task)进行分配的时候, 会计算出每个task要计算的是哪个分片的数据(partition)，Spark的task分配算法，会按照上面的顺序来进行分配。  
可能PROCESS_LOCAL节点的计算资源和计算能力都满了；Spark会等待一段时间，默认情况下是3s钟(不是绝对的，还有很多种情况，对不同的本地化级别，都会去等待)，到最后，就会选择一个比较差的本地化级别，比如说，将task分配到靠它要计算的数据所在节点，比较近的一个节点，然后进行计算。  

* 何时调节这个参数

观察日志，spark作业的运行日志，先用client模式，在本地就直接可以看到比较全的日志。日志里面会显示，starting task。。。，PROCESS LOCAL、NODE LOCAL
如果是发现，好多的级别都是NODE_LOCAL、ANY，那么最好就去调节一下数据本地化的等待时长。调节完，应该是要反复调节，每次调节完以后，再来运行，观察日志
`spark.locality.wait`, 3s, 6s, 10s...

#### JVM调优之降低cache操作的内存占比
spark中，堆内存又被划分成了两块儿，一块儿是专门用来给RDD的cache、persist操作进行RDD数据缓存用的；另外一块儿，用来给spark算子函数的运行使用的，存放函数中自己创建的对象。

默认情况下，给RDD cache操作的内存占比，是0.6，60%的内存都给了cache操作了。但是问题是，如果某些情况下，cache不是那么的紧张，问题在于task算子函数中创建的对象过多，然后内存又不太大，导致了频繁的minor gc，甚至频繁full gc，导致spark频繁的停止工作。性能影响会很大。

可以通过spark ui，如果是spark on yarn的话，那么就通过yarn的界面，去查看你的spark作业的运行统计。可以看到每个stage的运行情况，包括每个task的运行时间、gc时间等等。如果发现gc太频繁，时间太长。此时就可以适当调价这个比例。

降低cache操作的内存占比，大不了用persist操作，选择将一部分缓存的RDD数据写入磁盘，或者序列化方式，配合Kryo序列化类，减少RDD缓存的内存占用；降低cache操作内存占比；对应的，算子函数的内存占比就提升了。这个时候，可能，就可以减少minor gc的频率，同时减少full gc的频率。对性能的提升是有一定的帮助的。

`spark.storage.memoryFraction，0.6 -> 0.5 -> 0.4 -> 0.2`

#### JVM调优之调节Executor堆外内存与连接等待时长
* Executor堆外内存  

有时候，如果你的spark作业处理的数据量特别特别大，几亿数据量；然后spark作业一运行，时不时的报错，shuffle file cannot find，executor、task lost，out of memory（内存溢出）

可能是说executor的堆外内存不太够用，导致executor在运行的过程中，可能会内存溢出；然后可能导致后续的stage的task在运行的时候，可能要从一些executor中去拉取shuffle map output文件，
但是executor可能已经挂掉了，关联的block manager也没有了；所以可能会报shuffle output file not found；resubmitting task；executor lost；spark作业彻底崩溃。

```
--conf spark.yarn.executor.memoryOverhead=2048
```
spark-submit脚本里面，去用--conf的方式，去添加配置；一定要注意！！！切记，不是在你的spark作业代码中，用new SparkConf().set()这种方式去设置，不要这样去设置，是没有用的！一定要在spark-submit脚本中去设置。

默认情况下，这个堆外内存上限大概是300多M；通常项目，真正处理大数据的时候，这里都会出现问题，导致spark作业反复崩溃，无法运行；此时就会去调节这个参数，到至少1G（1024M），甚至说2G、4G

* 连接等待时长  

如果Executor远程从另一个Executor中拉取数据的时候，那个Executor正好在gc，此时呢，无法建立网络连接，会卡住；spark默认的网络连接的超时时长，是60s；如果卡住60s都无法建立连接的话，那么就宣告失败了。

碰到某某file。一串file id。uuid（dsfsfd-2342vs--sdf--sdfsd）。not found。file lost。很有可能是有那份数据的executor在jvm gc。所以拉取数据的时候，建立不了连接。然后超过默认60s以后，直接宣告失败。

```
--conf spark.core.connection.ack.wait.timeout=300
```
spark-submit脚本，切记，不是在new SparkConf().set()这种方式来设置的。通常来说，可以避免部分的偶尔出现的某某文件拉取失败，某某文件lost


#### Shuffle调优

##### 调节Map端内存缓冲与Reduce端内存占比

* 背景

默认情况下，shuffle的map task，输出到磁盘文件的时候，统一都会先写入每个task自己关联的一个内存缓冲区。这个缓冲区大小，默认是32kb。
每一次，当内存缓冲区满溢之后，才会进行spill操作，溢写操作，溢写到磁盘文件中去。

reduce端task，在拉取到数据之后，会用HashMap的数据格式，来对各个key对应的values进行汇聚。在进行汇聚、聚合等操作的时候，实际上，使用的就是自己对应的executor的内存，executor（jvm进程，堆），默认executor内存中划分给reduce task进行聚合的比例，是0.2。
问题来了，因为比例是0.2，所以，理论上，很有可能会出现，拉取过来的数据很多，那么在内存中，放不下；这个时候，默认的行为，就是说，将在内存放不下的数据，都spill（溢写）到磁盘文件中去。

* 调优
```
spark.shuffle.file.buffer
spark.shuffle.memoryFraction
```
在Spark UI或者是Yarn任务调度界面，如果发现shuffle过程中磁盘的write和read很大。这个时候，就意味着最好调节一些shuffle的参数。首先当然是考虑开启map端输出文件合并机制。
调节的时候的原则。spark.shuffle.file.buffer，每次扩大一倍，然后看看效果，64，128；spark.shuffle.memoryFraction，每次提高0.1，看看效果。










## 页面单跳转化率模块


## 各区域热门商品统计模块
技术点：Hive与MySQL异构数据源、RDD转换为DataFrame、注册和使用临时表、自定义UDAF聚合函数、自定义get_json_object等普通函数、Spark SQL的高级内置函数（if与case when等）、开窗函数（高端）  
Spark SQL数据倾斜解决方案


## 广告流量实时统计模块
技术点：动态黑名单机制（动态生成黑名单以及黑名单过滤）、transform、updateStateByKey、transform与Spark SQL整合、window滑动窗口、高性能写数据库  
HA方案：高可用性方案，3种  
性能调优：常用的性能调优的技巧

