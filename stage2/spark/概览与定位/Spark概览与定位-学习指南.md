# Spark 概览与定位 — 学习指南（D22 · L2）

> 配套练习：`WordCount` 三种写法（RDD / DataFrame / Spark SQL）  
> 前置知识：Flink 基础（你已经熟悉流处理）  
> 对标：Flink Checkpoint 指南同一风格，由浅入深

---

## 读前扫盲：Spark 是「批优先」的计算引擎，不是存储也不是调度器

很多初学者把 Spark 和 Hadoop 生态混淆。先记住：Spark **只做计算**，不做存储（数据在 HDFS/S3/Hive），不做资源分配（由 YARN/K8s 管）。

入门先记住三个结论：

| 结论 | 为什么重要 |
|------|------------|
| Spark 核心抽象从 RDD → DataFrame → Dataset 演进 | 越高层的抽象，Catalyst 优化空间越大，实际性能越好 |
| Spark 的流处理（Structured Streaming）本质是**微批**，不是逐条 | 决定了延迟下限在秒级，不能替代 Flink 做毫秒级实时 |
| 选型不看"谁更好"，看"延迟要求 × 数据量 × 团队栈" | 面试高频题，也是工程决策的核心 |

本指南用 **WordCount 三种写法** 串起抽象演进，用 **Spark vs Flink 对比表** 串起定位差异，用 **在线教育业务场景** 说明选型逻辑。

---

## Step 1 原理：架构三角 + 抽象演进

### ① Spark 在大数据体系中的位置

```
┌─────────────────────────────────────────────────────┐
│                   应用层 (Application)                │
│   ETL / Ad-hoc Query / ML / Graph / Streaming        │
├─────────────────────────────────────────────────────┤
│              Spark 计算引擎                           │
│  ┌──────────┬──────────┬──────────┬────────┬───────┐│
│  │Spark Core│Spark SQL │Structured│ MLlib  │GraphX ││
│  │          │          │Streaming │        │       ││
│  ├──────────┴──────────┴──────────┴────────┴───────┤│
│  │         Catalyst / Tungsten / AQE               ││
│  ├─────────────────────────────────────────────────┤│
│  │      RDD / DataFrame / Dataset 抽象层            ││
│  ├─────────────────────────────────────────────────┤│
│  │    Scheduler (DAG + Task) + Memory Manager      ││
│  └─────────────────────────────────────────────────┘│
├─────────────────────────────────────────────────────┤
│           Cluster Manager                            │
│     YARN / Standalone / K8s / Mesos                  │
├─────────────────────────────────────────────────────┤
│           Storage Layer                              │
│     HDFS / S3 / OSS / Hive Metastore / Kafka        │
└─────────────────────────────────────────────────────┘
```

### ② 架构三角：Driver / Executor / Cluster Manager

| 角色 | 职责 | 进程数 | 典型配置 |
|------|------|--------|----------|
| **Driver** | 运行 main()，构建 DAG，调度 Task，收集结果 | 1 | `spark.driver.memory=4g` |
| **Executor** | 执行 Task，缓存数据到内存/磁盘，汇报状态 | N（动态/静态） | `spark.executor.memory=8g, cores=4` |
| **Cluster Manager** | 为 Driver 分配 Executor 容器，管理生命周期 | 独立组件 | YARN RM / K8s API Server |

```
用户提交 spark-submit
        │
        ▼
   ┌─────────┐    申请资源     ┌──────────────────┐
   │  Driver  │◄──────────────►│  Cluster Manager  │
   │ (main()) │                │  (YARN/K8s/...)   │
   └────┬─────┘                └────────┬─────────┘
        │                               │
        │  分发 Task                     │ 启动 Executor
        ▼                               ▼
   ┌──────────────────────────────────────────┐
   │            Executors (N个)                 │
   │  ┌──────────┐ ┌──────────┐ ┌──────────┐ │
   │  │Executor-1│ │Executor-2│ │Executor-N│ │
   │  │ Cache+Task│ │Cache+Task│ │Cache+Task│ │
   │  └──────────┘ └──────────┘ └──────────┘ │
   └──────────────────────────────────────────┘
        │
        │ 结果汇报 / Shuffle
        ▼
   ┌─────────┐
   │  Driver  │ ← 收集最终结果、更新 DAG 状态
   └─────────┘
```

**Deploy Mode 选择**：

| 模式 | Driver 位置 | 适用场景 | 注意事项 |
|------|------------|---------|---------|
| **Client** | 提交机本地 | 交互式调试、spark-shell | Driver 挂了任务就没了 |
| **Cluster** | 集群内某个节点 | 生产作业 | Driver 有 HA，日志需从集群拉 |

> **记忆口诀**：**Driver 是大脑，Executor 是手脚，CM 是后勤** — 三角分工。

### ③ 核心抽象演进：RDD → DataFrame → Dataset

这是理解 Spark 的关键线索——**为什么要从 RDD 演进到 DataFrame/Dataset？**

```
演进动因：

  RDD（2012）:
    ✅ 类型安全（泛型）、容错（lineage）
    ❌ 无 schema → Catalyst 无法优化
    ❌ Java Serialization 开销大
    ❌ 每个 JVM 对象 16~40 字节头 → 内存浪费

  DataFrame（2015）:
    ✅ 带 schema → Catalyst 优化（谓词下推、列裁剪、Join Reorder）
    ✅ Tungsten 二进制编码 → 序列化开销降到 1/10
    ❌ 编译期不检查列名/类型（运行时才报错）

  Dataset（2016，仅 Scala/Java）:
    ✅ 兼具 RDD 类型安全 + DataFrame 优化
    ✅ Encoder 自动转换 JVM 对象 ↔ Tungsten 格式
    ❌ Python 不支持（PySpark 的 DataFrame 本质就是 Dataset[Row]）
```

三种写法对比（以 WordCount 为例）：

```scala
// ========== RDD 风格（理解原理用）==========
sc.textFile("hdfs:///data/words.txt")
  .flatMap(_.split(" "))
  .map((_, 1))
  .reduceByKey(_ + _)
  .collect()
// ❌ 无 schema，Catalyst 无法介入；Java 序列化开销大

// ========== DataFrame 风格（ETL 推荐）==========
val df = spark.read.parquet("hdfs:///data/words.parquet")
df.select(explode(split($"value", "\\s+")).alias("word"))
  .groupBy("word")
  .count()
  .show()
// ✅ Catalyst 优化全链路；Tungsten 编码；支持多数据源

// ========== Dataset 风格（Scala 业务代码推荐）==========
case class Word(w: String)
val ds: Dataset[Word] = spark.read.parquet("...").as[Word]
ds.groupBy(_.w).count().show()
// ✅ 编译期类型检查 + Catalyst 优化
```

**三者选型速查**：

| 维度 | RDD | DataFrame | Dataset |
|------|-----|-----------|---------|
| Schema | ❌ | ✅ | ✅ |
| 类型安全 | ✅ | ❌（运行时检查） | ✅（编译期检查） |
| Catalyst 优化 | ❌ | ✅ | ✅ |
| Tungsten 编码 | ❌ | ✅ | ✅ |
| Python 支持 | ✅ | ✅ | ❌（等同 DF） |
| **推荐场景** | 底层自定义算子 | **ETL/SQL（主力）** | Scala 类型安全业务 |

> **面试话术**：实际项目优先用 DataFrame/Dataset，因为 Catalyst 可以做谓词下推、列裁剪、Join Reorder。RDD 只在需要自定义 Partitioner 或 mapPartitions 时才用。

---

## Step 2 实操：本地装 Spark + 跑通 WordCount

### 安装（macOS / Docker 二选一）

```bash
# === macOS ===
brew install openjdk@11
export JAVA_HOME=$(/usr/libexec/java_home -v 11)

wget https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar xzf spark-3.5.1-bin-hadoop3.tgz
export SPARK_HOME=/path/to/spark-3.5.1-bin-hadoop3
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

# 验证
spark-shell --version   # Scala REPL
pyspark --version       # Python REPL

# === Docker 一键（无 Java 环境时推荐）===
docker run -it --rm \
  -p 4040:4040 \
  apache/spark:3.5.1 \
  /opt/spark/bin/pyspark
```

### 跑通 WordCount（三种写法）

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("WordCount-Demo") \
    .master("local[2]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")  # 减少 INFO 噪音

# ──── 方式一：DataFrame API（推荐）────
df = spark.read.text("data/words.txt")
result = (df
    .select(F.explode(F.split("value", "\\s+")).alias("word"))
    .filter(F.col("word") != "")
    .groupBy("word")
    .count()
    .orderBy(F.desc("count"))
)
result.show(20, truncate=False)

# ──── 方式二：RDD API（理解原理用）────
rdd_result = (spark.sparkContext
    .textFile("data/words.txt")
    .flatMap(lambda line: line.split())
    .map(lambda word: (word, 1))
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda x: x[1], ascending=False)
)
for item in rdd_result.take(20):
    print(item)

# ──── 方式三：Spark SQL ────
df.createOrReplaceTempView("words")
spark.sql("""
    SELECT word, COUNT(*) as cnt
    FROM (
        SELECT explode(split(value, '\\s+')) as word
        FROM words
    ) t
    WHERE word != ''
    GROUP BY word
    ORDER BY cnt DESC
""").show(20, truncate=False)

spark.stop()
```

### spark-submit 提交

```bash
# 本地模式
spark-submit --master local[4] --driver-memory 2g wordcount.py

# YARN Cluster（生产）
spark-submit \
  --master yarn --deploy-mode cluster \
  --num-executors 10 --executor-memory 8g --executor-cores 4 \
  --conf spark.sql.shuffle.partitions=200 \
  wordcount.py

# K8s
spark-submit \
  --master k8s://https://k8s-api:6443 --deploy-mode cluster \
  --conf spark.kubernetes.container.image=my-spark:3.5.1 \
  wordcount.py
```

### Spark UI 观察路径

**http://localhost:4040** → 核心 Tab：

| Tab | 关注点 | 本 Demo 如何观察 |
|-----|--------|------------------|
| **Jobs** | Job 列表、Stage 数、总耗时 | WordCount 应产生 3 个 Stage（2 次 Shuffle） |
| **Stages** | Task 数、Shuffle Read/Write | `reduceByKey` 和 `sortBy` 各产生一个 Shuffle Stage |
| **Executors** | 内存/GC/Shuffle/Task 分布 | 本地模式只有 1 个 Executor |
| **SQL** | 逻辑计划 → 优化计划 → 物理计划 | 看 Catalyst 做了什么优化 |
| **Environment** | 所有配置参数 | 对照 `spark-defaults.conf` |

### 执行过程拆解（为 D23 执行原理铺垫）

```
textFile / read.text
       │
       ▼
┌──────────────┐
│  flatMap /   │ ← Narrow Dependency（同分区内操作）
│  split       │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  map(w, 1)   │ ← Narrow Dependency
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ reduceByKey  │ ← ★ Wide Dependency → Shuffle Boundary
│              │   Stage 切分点！
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  sortBy      │ ← Wide Dependency → 又一次 Shuffle
└──────┬───────┘
       │
       ▼
   collect/take ← Action 触发整个 DAG 执行
```

**关键观察**：
- `flatMap`、`map` 是窄依赖，pipeline 在同一个 Task 里
- `reduceByKey` 是宽依赖 → 必须 Shuffle → **Stage 边界**
- `sortBy` 又是宽依赖 → 又一次 Shuffle → 又一个 Stage
- 整个 Job = **3 个 Stage**（这就是 D23 要深入讲的）

### Driver / Executor 日志

```bash
# Client 模式：日志在 stdout
spark-submit --master local[2] wordcount.py 2>&1 | tee app.log

# Cluster 模式（YARN）
yarn logs -applicationId application_1234567890_0001

# History Server（需先开启 eventLog）
# spark.eventLog.enabled=true
# spark.eventLog.dir=hdfs:///spark-logs
$SPARK_HOME/sbin/start-history-server.sh
# 访问 http://localhost:18080
```

---

## Step 3 对比：Spark vs Flink（面试高频）

### ① 计算模型本质差异

```
Spark（Micro-batch）:
  数据被切成小批次，每批当作一个 mini-job
  ──[batch1]──[batch2]──[batch3]──►
  延迟 = batch interval + 处理时间（秒~分钟级）

Flink（Native Streaming / Event-by-event）:
  每条数据到达即处理，watermark 驱动窗口
  ──e1──e2──e3──e4──e5──►
  延迟 = 处理时间（毫秒~亚秒级）
```

### ② 核心维度对比表

| 维度 | Spark | Flink | 选型启示 |
|------|-------|-------|---------|
| **计算模型** | Micro-batch / Batch | Event-driven + Batch | 延迟敏感选 Flink |
| **最低延迟** | ~100ms（SS trigger） | ~ms 级 | 风控/实时告警选 Flink |
| **吞吐量** | 极高（批量优化） | 高（单条有开销） | 纯离线大吞吐选 Spark |
| **状态管理** | RDD Lineage + Checkpoint | KeyedState + RocksDB + 增量 CP | 大状态场景 Flink 更稳 |
| **Exactly-once** | Sink 端保证（幂等/事务） | Source→Sink 全链路 2PC | 金融级一致性选 Flink |
| **SQL 成熟度** | 非常成熟，Hive 兼容极好 | 快速追赶，CDC 原生支持 | 数仓 ETL 选 Spark SQL |
| **ML 能力** | MLlib 内置 | 无（需外部对接） | ML Pipeline 选 Spark |
| **图计算** | GraphX | 无 | 图算法选 Spark 或专用引擎 |
| **CDC** | 有限（需第三方） | 原生 CDC Connector | 数据库同步选 Flink |
| **CEP** | 无 | 强大（Pattern API + MATCH_RECOGNIZE） | 复杂事件处理选 Flink |
| **运维** | YARN/K8s 成熟 | StatefulSet + Savepoint 较复杂 | 团队经验也是考量因素 |
| **社区/文档** | 极其丰富 | 活跃但中文资源偏少 | 新人上手 Spark 更快 |

### ③ "怎么选"的决策树

```
你的需求是什么？
    │
    ├── 纯离线 ETL / 数仓报表 / ML训练
    │       → Spark（成熟、吞吐高、生态完善）
    │
    ├── 实时指标 / 实时监控 / CDC入仓
    │       → Flink（低延迟、精确一次、状态强大）
    │
    ├── 既要离线又要实时，且数据量不大
    │       → Structured Streaming（一套代码兼顾）
    │
    ├── 既要离线又要实时，数据量大且要求严格一致
    │       → Flink 实时 + Spark 离线 + 湖表统一
    │         （Paimon/Iceberg 流批一体方案）
    │
    └── 图计算 / 迭代算法
            → Spark GraphX 或专用图引擎
```

> **记忆口诀**：**批选 Spark、流选 Flink、湖表统一两手抓**。

---

## Step 4 陷阱：别把 Structured Streaming 当 Flink

### 最大的认知误区

很多人以为 Structured Streaming = Flink 的流处理，这是**错的**。

```
Structured Streaming 的本质：
  ┌──────────────────────────────────────────────┐
  │  trigger interval = 1s                       │
  │                                              │
  │  ──[1s数据]──→ 当 mini-batch 处理 ──→ 输出   │
  │  ──[1s数据]──→ 当 mini-batch 处理 ──→ 输出   │
  │                                              │
  │  即使 trigger=1s，每条数据仍要等批次凑齐     │
  │  延迟下限 = trigger interval + job 调度开销  │
  └──────────────────────────────────────────────┘

Flink 的本质：
  ┌──────────────────────────────────────────────┐
  │  ──e1─→ 立即处理 ──→ 输出                    │
  │  ──e2─→ 立即处理 ──→ 输出                    │
  │  ──e3─→ 立即处理 ──→ 输出                    │
  │                                              │
  │  逐条处理，延迟 = 纯计算时间                 │
  └──────────────────────────────────────────────┘
```

| 场景 | Structured Streaming 够用？ | 该用 Flink？ |
|------|---------------------------|-------------|
| 秒级报表、状态不大 | ✅ 够用，开发成本低 | ❌ 杀鸡用牛刀 |
| 窗口 ≥ 小时级、状态 GB 级 | ❌ CP 恢复慢、状态管理弱 | ✅ 增量 CP + RocksDB |
| 毫秒级延迟、复杂事件 | ❌ 微批延迟下限太高 | ✅ 逐条 + CEP |
| 长会话窗口、大量用户状态 | ❌ Checkpoint 全量、恢复慢 | ✅ KeyedState + 增量 |
| CDC 同步、精确一次写入 | ❌ 端到端保证依赖 Sink 幂等 | ✅ 2PC 全链路 |

### 其他常见陷阱

| 陷阱 | 说明 | 正确认知 |
|------|------|---------|
| "Spark 比 Flink 快" | 批处理吞吐 Spark 更高，但延迟 Flink 更低 | 不是快慢之分，是模型差异 |
| "Flink 比 Spark 先进" | 它们解决不同层次的问题 | 选型看约束条件 |
| "Spark 不能做流" | SS 可以做流，但是微批流 | 明确"微批 vs 逐条"的差异 |
| "RDD 过时了" | RDD 是底层基石，DataFrame 构建在 RDD 之上 | RDD 在自定义算子时仍有用 |

---

## Step 5 面试话术

### 问：Spark 和 Flink 怎么选？

> 我们选型的核心原则是**"按场景匹配计算模型"**，不是简单比谁更好。
>
> **第一，看延迟要求**。秒级以上的统计报表、离线 ETL，用 Spark——micro-batch 吞吐高，SQL 生态成熟。毫秒级的实时告警、风控规则引擎，用 Flink——逐条处理，延迟下限远低于 Spark。
>
> **第二，看状态复杂度**。Flink 的 KeyedState + RocksDB 能撑 TB 级状态，增量 checkpoint 比 Spark 的全量更适合长窗口。如果涉及超长会话窗口、大量用户画像状态，优先 Flink。
>
> **第三，看一致性要求**。Flink 端到端 exactly-once 靠两阶段提交，source 到 sink 全链路。Spark SS 的 exactly-once 更多依赖 sink 端幂等或事务。金融级对账我们倾向 Flink。
>
> **第四，看团队和运维**。Spark 在 YARN 上跑了很多年，排查资料多。Flink 的状态管理和 savepoint 升级需要更多经验。团队刚起步可以先用 SS 过渡。
>
> **最后，我们在推进湖表方案**（Paimon/Iceberg）——Flink 实时写入湖表，Spark 离线分析同一张表，在存储层实现统一，减少 Lambda 架构两套管道。

### 追问：Structured Streaming 和 Flink 有什么区别？

> SS 本质还是微批，只是把批次间隔缩到 trigger interval。延迟在秒级、状态不大的场景它够用且开发成本低。但窗口超过小时级、状态达到 GB 级时，SS 的 checkpoint 恢复很慢，状态管理也不如 Flink 的 RocksDB + 增量 CP，这时候就该换 Flink。

### 追问：为什么实时选 Flink、离线大批量 ETL 选 Spark？

> **离线 ETL 选 Spark** 的工程原因：
> 1. **吞吐优先**：Spark 的 micro-batch 模型天然适合大吞吐，executor 可以充分利用内存缓存中间结果，减少磁盘 IO。
> 2. **SQL 生态**：Spark SQL 与 Hive 高度兼容，Catalyst 优化器自动做谓词下推、列裁剪、Join Reorder，开发者不需要手写调优。
> 3. **ML 一体化**：ETL 完直接接 MLlib 做特征工程，一套代码搞定。
>
> **实时选 Flink** 的工程原因：
> 1. **延迟下限**：逐条处理，毫秒级响应，微批模型做不了。
> 2. **大状态**：RocksDB 后端 + 增量 checkpoint，支撑 TB 级状态。
> 3. **端到端一致性**：2PC 从 source 到 sink 全链路 exactly-once，不需要依赖 sink 幂等。
> 4. **CDC 原生**：直接消费 MySQL/PG 的 binlog，这是 Spark 做不了的。

### 追问：Spark 为什么比 MapReduce 快？

> 两个原因：① 中间结果缓存在内存而非落盘（MapReduce 每个 stage 都写 HDFS）；② DAG 调度粒度更细，pipeline 化执行减少了不必要的 barrier。

---

## Step 6 在线教育业务场景（选型三角）

> 以下三个场景对应 Spark vs Flink 选型的三种典型决策：  
> **纯离线 ETL → Spark** / **实时告警 → Flink** / **流批一体 → 湖表统一**

---

### 场景一：T+1 学员画像报表 → Spark

#### 业务背景

教务系统每天凌晨跑批：汇总学员近 30 天的完课率、平均学习时长、活跃度，生成家长可见的「月度学情报告」。数据量 5 亿条/天。

#### 为什么选 Spark

| 维度 | 分析 |
|------|------|
| **延迟要求** | T+1（次日凌晨出结果即可），不敏感 |
| **数据量** | 5 亿条/天，需要高吞吐批处理 |
| **计算复杂度** | 多表 Join + 多维聚合，Spark SQL + Catalyst 自动优化 |

#### 架构

```
Hive(学员心跳表) ──┐
                    ├── Spark SQL Join + 聚合 ──→ Hive(月度画像表) ──→ BI 报表
Hive(完课记录表) ──┘
```

#### 踩坑

- Spark SQL Join 默认 Broadcast 阈值 10MB，大表 Join 要调 `spark.sql.autoBroadcastJoinThreshold`。
- 写出分区过多导致小文件问题——需 `repartition` 或 `coalesce` 后再写。

---

### 场景二：实时学习异常告警 → Flink

#### 业务背景

学员在线学习时，如果连续 10 分钟无任何交互（无心跳、无翻页、无答题），实时推送告警给班主任。要求秒级响应。

#### 为什么选 Flink

| 维度 | 分析 |
|------|------|
| **延迟要求** | 秒级，10 分钟无交互即触发 |
| **状态** | 每个学员一个 Session 状态，在线学员 50 万 |
| **CEP** | 复杂事件模式（连续 N 条心跳缺失），Flink Pattern API 原生支持 |

#### 架构

```
Kafka(学习心跳) ──→ Flink CEP Pattern(10min 无交互) ──→ Kafka(告警) ──→ 推送
```

#### 踩坑

- Spark SS 做不到秒级 + 复杂 CEP，微批延迟下限太高。
- Flink CEP 的 Pattern 超时后状态要设 TTL，否则内存涨。

---

### 场景三：实时+离线统一 → 湖表（Paimon/Iceberg）

#### 业务背景

同一份学员学习数据，既要**实时大屏**（秒级刷新），又要**离线回测**（算法团队拿历史数据训练推荐模型）。传统 Lambda 架构要维护两套管道、两套口径。

#### 为什么选湖表统一

| 维度 | 分析 |
|------|------|
| **痛点** | Lambda 双链路口径不一致、运维成本高 |
| **方案** | Paimon 统一存储 → Flink 实时写入 + 流读大屏 / Spark 批读回测 |
| **收益** | 一份数据、一套口径、两种消费 |

#### 架构

```
CDC/学习心跳 ──→ Flink ──→ Paimon(ODS/DWD)
                              │
                     ┌────────┴────────┐
                     │                  │
              Flink 流读            Spark 批读
              (实时大屏)           (算法回测/月报)
```

**与后续章节的关联**：这个场景就是 D27（流批一体叙事）和 D41（流批一体落地架构）要深入展开的。

---

### 三场景对照总表

| 场景 | 选型 | 典型症状 | 核心技术 | 与后续 Demo 对应 |
|------|------|----------|----------|------------------|
| T+1 画像报表 | **Spark** | 大吞吐批处理 | Spark SQL + Catalyst | D24 Spark SQL 实操 |
| 实时异常告警 | **Flink** | 秒级 + CEP | Flink CEP + State | 阶段一 Flink Demo |
| 流批统一 | **湖表** | 双链路口径不一致 | Paimon + Flink/Spark | D27/D41 流批一体 |

---

## 验收清单

| # | 验收项 | 验证方式 |
|---|--------|----------|
| ① | 能画 Spark 架构三角 | Step 1② ASCII 图 |
| ② | 能讲 RDD → DF → Dataset 演进原因 | Step 1③ 三种写法对比 |
| ③ | 本地跑通 WordCount + 看 Spark UI | Step 2 实操 |
| ④ | 能脱稿讲 Spark vs Flink 对比表 | Step 3② 12 维对比 |
| ⑤ | 能讲清 SS ≠ Flink（微批 vs 逐条） | Step 4 陷阱 |
| ⑥ | 能按决策树做选型 | Step 3③ 决策树 |
| ⑦ | 能讲三个业务场景的选型逻辑 | Step 6 三场景 |

---

## 运行速查

```bash
# 1. 启动 REPL
pyspark --master local[2]

# 2. 提交作业
spark-submit --master local[4] --driver-memory 2g wordcount.py

# 3. Spark UI
open http://localhost:4040

# 4. History Server（需配置 eventLog）
$SPARK_HOME/sbin/start-history-server.sh
open http://localhost:18080
```

---

## 与其他章节的关系

| 后续章节 | 本章铺垫 |
|----------|---------|
| D23 Spark 执行原理 | Step 2 的 Stage 划分图 → 展开讲 Job/Stage/Task |
| D24 Spark SQL 实操 | Step 1③ 的 DataFrame API → 完整 ETL demo |
| D25 Spark 调优 | Step 3② 的 Shuffle → 展开讲倾斜/AQE/Broadcast |
| D27 流批一体叙事 | Step 6 场景三 → Lambda→Kappa→湖仓演进 |
| D41 流批一体落地 | Step 6 场景三 → 端到端架构设计 |
