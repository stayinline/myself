# Spark 执行原理 — 学习指南（D23 · L2）

> 配套代码：`execution-demo.py`（groupBy + Join + 倾斜构造）  
> 前置知识：D22 Spark 概览与定位（架构三角、RDD/DF/Dataset）  
> 对标：Flink Checkpoint 指南同一风格，由浅入深

---

## 读前扫盲：一个 Spark 作业是怎么跑起来的

Spark 不是逐条处理数据的流引擎，而是**批调度模型**：用户写的是 Transformation 链，真正触发计算的是一个 Action；Action 产生 Job，Job 按 Shuffle 边界切成 Stage，Stage 再拆成并行 Task 分发到 Executor。

入门先记住三个结论：

| 结论 | 为什么重要 |
|------|------------|
| **宽依赖 = Shuffle = Stage 切分点** | 面试必问"Stage 怎么来的"，答案就是遇到宽依赖就切一刀 |
| **Shuffle 是性能瓶颈的万恶之源** | 磁盘 IO + 网络传输 + 序列化三重开销；调优 80% 围绕它展开 |
| **Lineage 重算是容错基石，Checkpoint 截断长链路** | 窄依赖只重算丢失分区；宽依赖要回退整个上游 Stage |

本指南用 **groupBy/Join 作业的 Spark UI** 串起原理，用 **宽窄依赖对比实验** 串起性能认知，用 **在线教育业务场景** 说明工程落地。

---

## Step 1 原理：Job → Stage → Task 三级划分

### ① 从 Action 到 Task 的全链路

```
Action (collect / count / save...)
        │
        ▼
     ┌─────┐
     │ Job │ ← 一次 Action = 一个 Job
     └──┬──┘
        │ DAGScheduler 反向遍历依赖链
        │ 遇到 ShuffleDependency → 切新 Stage
        ▼
   ┌─────────┐  ┌─────────┐  ┌─────────┐
   │ Stage 0 │→ │ Stage 1 │→ │ Stage 2 │
   └────┬────┘  └────┬────┘  └────┬────┘
        │            │            │
   ┌────┴────┐  ┌────┴────┐  ┌────┴────┐
   │Task│Task│  │Task│Task│  │Task│Task│ ← 每个分区 = 一个 Task
   └─────────┘  └─────────┘  └─────────┘
```

| 层级 | 含义 | 数量决定因素 |
|------|------|-------------|
| **Job** | 一次 Action 触发的完整计算图 | Action 调用次数 |
| **Stage** | 无 Shuffle 的最大子图 | 宽依赖（ShuffleDependency）个数 + 1 |
| **Task** | Executor 上最小执行单元 | Stage 输出 RDD 的分区数 |

### ② 宽依赖 vs 窄依赖：Stage 切分的唯一依据

```
窄依赖（Narrow Dependency）:           宽依赖（Wide Dependency）:
父分区 → 最多一个子分区                父分区 → 多个子分区

Parent         Child                  Parent          Child
┌──────┐      ┌──────┐               ┌──────┐       ┌──────┐
│ P-0  │─────►│ P-0  │               │ P-0  │──┬───►│ P-0  │
├──────┤      ├──────┤               ├──────┤  │    ├──────┤
│ P-1  │─────►│ P-1  │               │ P-1  │──┼───►│ P-1  │
├──────┤      ├──────┤               ├──────┤  │    ├──────┤
│ P-2  │─────►│ P-2  │               │ P-2  │──┘───►│ P-2  │
└──────┘      └──────┘               └──────┘       └──────┘

map/filter/flatMap                   groupByKey/reduceByKey
mapPartitions                        join(非broadcast)/sortByKey
                                     repartition/coalesce
★ 同 Task 内 pipeline               ★ 必须 Shuffle → Stage 边界
```

### ③ Stage 类型与 Task 类型

| Stage 类型 | 说明 | 对应 Task |
|-----------|------|----------|
| **ShuffleMapStage** | 有下游 Stage 依赖，输出 Shuffle 文件 | ShuffleMapTask |
| **ResultStage** | 最终 Stage，产出 Action 结果 | ResultTask |

### ④ 两个调度器分工

```
Action → DAGScheduler(Stage划分+依赖管理) → TaskScheduler(Task分发+重试+推测) → Executor
              ↑                                                        │
              └──────────── statusUpdate(SUCCESS) ◄────────────────────┘
```

| 调度器 | 职责 | 关键能力 |
|--------|------|---------|
| **DAGScheduler** | 逻辑层：Stage 划分、依赖管理 | 反向遍历依赖链，遇宽依赖切 Stage |
| **TaskScheduler** | 物理层：Task 分发、重试、推测执行 | FIFO/Fair 调度；Speculative Execution 缓解长尾 |

> **记忆口诀**：**Action 出 Job，宽依切 Stage，分区定 Task；DAG 画蓝图，TS 派工人**。

---

## Step 2 实操：跑 groupBy/Join 作业，看 Spark UI

### 构造数据 + 运行含 Shuffle 的作业

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("execution-demo").master("local[4]") \
    .config("spark.ui.port", "4040") \
    .config("spark.sql.shuffle.partitions", "8").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 订单表 100万行 + 用户表 1000行
orders = spark.range(0, 1000000).select(
    F.col("id").alias("order_id"),
    (F.col("id") % 1000).alias("user_id"),
    (F.rand() * 1000).cast("decimal(10,2)").alias("amount"))
users = spark.range(0, 1000).select(
    F.col("id").alias("user_id"),
    F.concat(F.lit("user_"), F.col("id")).alias("user_name"))
orders.cache(); users.cache()
orders.count()  # 触发缓存

# groupBy → 必然触发 Shuffle
orders.groupBy("user_id").agg(F.count("*"), F.sum("amount")).show()

# Join：开启 Broadcast vs 关闭 Broadcast 对比
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")
orders.join(users, on="user_id").show()          # BroadcastHashJoin
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
orders.join(users, on="user_id").explain("formatted")  # SortMergeJoin
```

### Spark UI 观察路径

**http://localhost:4040** → 核心关注点：

| Tab | 关注指标 | 本 Demo 如何验证 |
|-----|---------|-----------------|
| **Jobs** | Job 数、Stage 数、Duration | groupBy → 2 Stage；Shuffle Join → 3 Stage |
| **Stages** | Summary Metrics Min/Max、Shuffle Read/Write | Max >> Min → 数据倾斜信号 |
| **SQL** | Physical Plan 中的 `Exchange` 节点 | `Exchange hashpartitioning` = Shuffle 发生点 |
| **Executors** | GC Time、Shuffle Spill | Spill > 0 → 内存不够，考虑增大 executor-memory |

### SQL Tab 关键输出

```
== Physical Plan ==
*(2) SortAggregate [user_id], [count(*) AS order_cnt, sum(amount) AS total_amount]
   +- Exchange hashpartitioning(user_id, 8)    ← ★ Shuffle 发生点！
      +- *(1) Filter isnotnull(user_id)
         +- FileScan parquet [...]
```

> **面试话术**：在 Spark UI 的 SQL Tab 里看到 `Exchange` 就意味着发生了 Shuffle。`hashpartitioning` 表示按哈希分区做数据重分布，这就是宽依赖的物理体现。

---

## Step 3 对比：宽窄依赖对性能与容错的影响

### ① 性能实测：窄依赖链 vs 宽依赖操作

```python
import time
# 窄依赖链（pipeline，无 Shuffle）
start = time.time()
orders.filter(F.col("amount") > 100) \
      .withColumn("total", F.col("amount") * 1.1).count()
print(f"Narrow: {time.time()-start:.2f}s")   # ≈ 1.2s

# 宽依赖（groupBy → Shuffle）
start = time.time()
orders.groupBy("user_id").agg(F.sum("amount")).count()
print(f"Wide:   {time.time()-start:.2f}s")   # ≈ 4.8s
```

> 结论：窄依赖 pipeline 在一个 Stage 内完成；引入 Shuffle 后耗时增加 3~5 倍。

### ② 为什么 Shuffle 是性能瓶颈

```
Shuffle 三重开销：

  ① 磁盘 IO
     Map Task 输出 → spill 到本地磁盘 → merge sort → 生成 data+index 文件
     
  ② 网络传输
     Reduce Task 从所有上游 Map Task 拉取对应 partition 的数据块
     
  ③ 序列化/反序列化
     JVM 对象 → Tungsten 二进制 → 网络 → 反序列化为 JVM 对象
```

| 维度 | 窄依赖 | 宽依赖（Shuffle） |
|------|--------|------------------|
| 执行方式 | 同 Task 内 pipeline | 上游写完 → barrier → 下游才读 |
| 中间数据 | 内存传递，不落盘 | 落盘 + 网络传输 |
| 并行度 | 上下游同时执行 | Stage 间串行 |
| 容错粒度 | 只重算丢失分区 | 回退整个上游 Stage |
| 耗时占比 | 通常 < 10% | 通常占 Job 总时长 50~80% |

### ③ Lineage 重算与容错

```
窄依赖容错（轻量）：
  RDD-C P-2 丢失 → 回溯 lineage → 只重算 RDD-B P-2 → RDD-A P-2
  影响范围：单个分区链路

宽依赖容错（重量）：
  Shuffle 文件丢失 → 回退整个 ShuffleMapStage → 所有 Map Task 重跑
  影响范围：整个上游 Stage
```

| 维度 | Cache/Persist | Checkpoint |
|------|--------------|------------|
| 存储位置 | Executor 内存/磁盘 | HDFS/S3（可靠存储） |
| 用途 | 加速重复使用 | **截断 lineage**，防止栈溢出 |
| 容错能力 | Executor 挂了需重算 | 直接从 HDFS 恢复 |
| 适用场景 | 迭代计算中间结果复用 | PageRank 等长链路迭代算法 |

> **面试话术**：Cache 是为了性能，Checkpoint 是为了可靠性。长链路迭代算法一定要做 checkpoint，否则 lineage 太长会导致 StackOverflowError 或恢复时间过长。Shuffle 文件丢了不用从头算，只需回退到上一个 Shuffle 边界重跑对应的 ShuffleMapStage。

---

## Step 4 陷阱：Shuffle 落盘、网络开销与数据倾斜

### ① Shuffle 流程与落盘机制

```
Map Task 输出记录
       │
       ▼
┌──────────────────┐
│ AppendOnlyMap    │ ← 内存中聚合（如果有 map-side combine）
└────────┬─────────┘
         │ 内存不足时 spill
         ▼
┌──────────────────┐
│ Spill to Disk    │ ← 每次 spill 生成一个临时排序文件
└────────┬─────────┘
         │ 所有数据处理完
         ▼
┌──────────────────┐
│ External Sorter  │ ← 多路归并所有 spill 文件
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Data File + Index│ ← 按 reducer partition 顺序排列
└──────────────────┘
         │
         │ Network Pull
         ▼
  Reduce Task 拉取 + Merge + 处理
```

### ② 三种 Shuffle Manager 演进

| 管理器 | 版本 | 特点 | 状态 |
|--------|------|------|------|
| HashShuffleManager | < 1.2 | 每 Task × 每 Reducer = 一个文件 | ❌ 已废弃 |
| SortShuffleManager | ≥ 1.2 | 先排序再写，减少文件数 | ✅ 默认 |
| Tungsten-Sort | ≥ 1.5 | 在序列化二进制上排序，避免反序列化 | ✅ 自动启用 |

**Bypass Merge Sort**：分区数 ≤ `bypassMergeThreshold`(200) 且无 map-side combine 时跳过排序，省开销但多小文件。

### ③ 数据倾斜在 Shuffle 阶段爆发

```
正常情况（均匀分布）：
  Task-0: ████░░░░  2s
  Task-1: █████░░░  2.5s
  Task-2: ████░░░░  2s
  Task-3: █████░░░  2.5s

倾斜情况（热点 key 集中在一个分区）：
  Task-0: ██░░░░░░  1s
  Task-1: ██░░░░░░  1s
  Task-2: ██████████████████████  15s  ← 长尾！
  Task-3: ██░░░░░░  1s
```

诊断三板斧：

| 方法 | 操作 | 判定标准 |
|------|------|---------|
| **UI Summary Metrics** | Stage 详情 → Duration Min/Max | Max / Median > 3x |
| **Shuffle Read 分布** | Stage 详情 → Tasks 列表 | 某 Task Shuffle Read >> 其他 |
| **Key 分布检查** | `df.groupBy(key).count().orderBy(desc("count"))` | Top key count 远超均值 |

### ④ 倾斜解决方案速查

| 方案 | 原理 | 适用条件 | 复杂度 |
|------|------|---------|--------|
| **Salting（加盐）** | 热点 key 加随机前缀打散 | 聚合类操作 | ⭐⭐⭐ |
| **Broadcast Join** | 小表广播到所有节点 | 一大一小表 join | ⭐ |
| **两阶段聚合** | 先局部聚合再全局聚合 | groupBy/countDistinct | ⭐⭐⭐ |
| **过滤热点 key** | 单独处理热点，其余正常聚合 | 热点 key 可枚举 | ⭐⭐ |
| **AQE Skew Join** | Spark 3.x 自动检测拆分倾斜分区 | Spark ≥ 3.0 | ⭐ |
| **调整分区数** | 增大 shuffle partitions | 轻度倾斜 | ⭐ |

> **记忆口诀**：**Shuffle 三害——落盘、传网、倾斜来；加盐广播两阶段，AQE 自动最省心**。

---

## Step 5 面试话术

### 问：一个 Spark 作业从提交到出结果经历了什么？

> 我按 **提交 → 划分 → 调度 → 执行 → 输出** 五步来讲。
>
> **第一步，提交**。用户调用 Action（如 collect/count），触发 `DAGScheduler.submitJob()`。Transformation 是惰性的，只有 Action 才真正触发计算。
>
> **第二步，划分**。DAGScheduler 从目标 RDD 反向遍历依赖链，遇到**宽依赖（ShuffleDependency）**就切出一个新的 Stage。最终得到一组 Stage DAG。每个 Stage 内的操作都是窄依赖，可以 pipeline 化执行。Stage 数量 = 宽依赖数 + 1。
>
> **第三步，调度**。DAGScheduler 把每个 Stage 拆成 TaskSet（Task 数 = 输出 RDD 分区数），交给 TaskScheduler。TaskScheduler 通过 SchedulerBackend 根据资源情况将 Task 分发到各 Executor。调度策略可以是 FIFO 或 Fair。
>
> **第四步，执行**。Executor 收到 Task 后执行计算。ShuffleMapTask 的输出写入本地磁盘生成 Shuffle 文件；Reduce Task 通过网络从上游拉取对应的 Shuffle 数据块。如果某个 Task 失败，TaskScheduler 会自动重试；开启推测执行还能缓解长尾问题。
>
> **第五步，输出**。ResultStage 的 ResultTask 将结果返回给 Driver。如果是 save 类 Action，则由 Executor 直接写入外部存储。
>
> 整个过程的核心就是：**Action 触发 Job → 宽依赖切 Stage → 分区定 Task → DAGScheduler 管逻辑 → TaskScheduler 管物理**。

### 追问：reduceByKey 为什么比 groupByKey 好？

> reduceByKey 有 **map-side combine**——在 Shuffle 之前先在 Map 端做局部聚合，大幅减少过网络的数据量。groupByKey 不做预聚合，所有原始 KV 都要过 Shuffle，网络开销可能是 reduceByKey 的数十倍。生产环境能用 reduceByKey 就不用 groupByKey。

### 追问：Shuffle 文件丢了怎么办？

> Shuffle 文件存在 Executor 本地磁盘。如果该 Executor 挂了，MapOutputTracker 检测到 loss，DAGScheduler 重新提交对应的 ShuffleMapStage，下游 Task 等新文件就绪后才继续。**不需要从头算**，只需回退到上一个 Shuffle 边界。这就是 Spark 容错的精髓——Shuffle 边界既是性能分割线，也是容错分割线。

### 📌 加分：Spark Shuffle vs Flink 网络传输/反压

> Spark 是 **batch barrier** 模型：上游 Stage 全部完成后下游才开始拉取 Shuffle 数据，中间有等待间隙。Flink 是 **pipeline** 模型：上下游 subtask 同时运行，通过 credit-based 流控实现背压——消费端变慢时反压信号沿数据流传到 source，source 降速读取。
>
> 所以 Spark 不存在传统意义的"反压"，因为 Stage 之间天然隔离。但也因此资源利用率不如 Flink——Stage 切换时集群可能部分空闲。Flink 资源利用率高，但背压排查更复杂，可能出现在链路任何环节。

| 维度 | Spark Shuffle | Flink Network |
|------|--------------|---------------|
| 传输时机 | Barrier：上游完成后下游才拉 | Pipeline：上下游并行 |
| 中间存储 | 本地磁盘文件 | 内存 Buffer（溢写到磁盘） |
| 流控 | Pull 模式，按需请求 | Credit-based push-pull 混合 |
| 背压 | 不存在（barrier 天然隔离） | 原生支持，反压到 source |
| 容错 | 重算上游 Stage | Checkpoint barrier + replay |

---

## Step 6 在线教育业务案例（执行原理三角）

> 以下三个场景是在线教育平台里 **Shuffle 问题最高发** 的业务，分别对应：  
> **groupBy 聚合倾斜** / **大表 Join Shuffle 爆炸** / **长链路 Lineage 过重**。

---

### 案例一：学员完课统计 groupBy — Shuffle 倾斜

#### 业务背景

每日凌晨跑批：按 `student_id` 聚合全量学习心跳（5 亿条/天），生成「每人当日完课数 + 学习时长」。少数活跃学员（如刷题备考）心跳量是普通学员的 100 倍。

#### 故障现象

```
Stage 1 Summary Metrics:
  Duration Min=0.8s, Max=42min → 严重倾斜
  Shuffle Read Min=12MB, Max=8.2GB
  整个 Job 被一个 Task 拖住，后续报表延迟 2 小时
```

#### 三要素

| 维度 | 分析 |
|------|------|
| 根因 | 热点 student_id 集中在一个 Shuffle 分区 → 单 Task 数据量百倍于其他 |
| 信号 | UI Stage 详情 Max/Median > 10x；Shuffle Read 分布极端不均 |
| 对策 | ① AQE Skew Join（Spark 3.x 首选）；② Salting 加盐打散热点 key；③ 两阶段聚合 |

> **踩坑**：盲目增大 `shuffle.partitions` 只缓解轻度倾斜；`reduceByKey` 减少 Shuffle 量但不解决倾斜本身。

---

### 案例二：订单 × 课程大表 Join — Shuffle 风暴

#### 业务背景

周报需要关联「订单明细表」（2 亿行）和「课程维表」（5 万行），生成带课程名称的订单报表。初始未设 Broadcast 阈值，走了 Shuffle Hash Join。

#### 故障现象

```
Stage 1 Shuffle Write: 45GB
Stage 2 Shuffle Read: 45GB
Job 总时长 35min，其中 Shuffle 占 28min（80%）
```

#### 三要素

| 维度 | 分析 |
|------|------|
| 根因 | 课程维表仅 5 万行但未触发 Broadcast → 两张表都做了全量 Shuffle |
| 信号 | SQL Tab 看到 `SortMergeJoin` 而非 `BroadcastHashJoin` |
| 对策 | 调 `spark.sql.autoBroadcastJoinThreshold=50MB` 让维表走 Broadcast；或手动 `broadcast()` hint |

> **踩坑**：Broadcast 太大撑爆 Driver 内存；生产建议显式 `broadcast()` hint 避免执行计划不稳定。

---

### 案例三：PageRank 迭代算法 — Lineage 过长导致 StackOverflow

#### 业务背景

推荐团队用 Spark GraphX 跑课程关联图的 PageRank，迭代 50 轮。每轮迭代产生一个新的 RDD，lineage 链长达 50 层。

#### 故障现象

```
第 30 轮迭代后 Executor OOM / StackOverflowError
Task 失败重试也要回溯 30 层 lineage → 恢复极慢
```

#### 三要素

| 维度 | 分析 |
|------|------|
| 根因 | 迭代算法每轮追加 lineage，不做 checkpoint → 链路指数膨胀 |
| 信号 | StackOverflowError；Task 重试耗时逐轮递增 |
| 对策 | 每 N 轮做一次 `rdd.checkpoint()` 截断 lineage；配合 `cache()` 避免重复计算 |

> **踩坑**：`cache()` ≠ `checkpoint()`——cache 不截断 lineage；checkpoint 写 HDFS 有开销，通常 5~10 轮做一次。

---

### 三案例对照总表

| 案例 | 执行原理主题 | 典型症状 | 核心对策 |
|------|-------------|----------|---------|
| 完课统计 groupBy | **Shuffle 倾斜** | 单 Task 耗时百倍于其他 | AQE / Salting / 两阶段聚合 |
| 订单×课程 Join | **Shuffle 风暴** | Shuffle 占 Job 80% 时长 | Broadcast Join |
| PageRank 迭代 | **Lineage 过长** | StackOverflow / 恢复慢 | 定期 Checkpoint 截断 |

---

## 验收清单

| # | 验收项 | 验证方式 |
|---|--------|----------|
| ① | 能画 Job → Stage → Task 划分图 | Step 1① ASCII 图 |
| ② | 能解释 Shuffle 触发点（宽依赖） | Step 1② 宽窄依赖对比 |
| ③ | 能在 Spark UI 中找到 Shuffle 发生的位置 | Step 2 SQL Tab `Exchange` 节点 |
| ④ | 能讲清 reduceByKey vs groupByKey 差异 | Step 3② + Step 5 追问 |
| ⑤ | 能诊断数据倾斜并给出至少两种解法 | Step 4③④ + 案例一 |
| ⑥ | 能脱稿讲"一个 Spark 作业从提交到出结果" | Step 5 面试话术 |
| ⑦ | 能对比 Spark Shuffle 与 Flink 网络传输 | Step 5 加分点 |

---

## 运行速查

```bash
# 1. 启动 PySpark REPL
pyspark --master local[4] --conf spark.ui.port=4040

# 2. 提交 Demo 脚本
spark-submit --master local[4] --driver-memory 2g execution-demo.py

# 3. 打开 Spark UI
open http://localhost:4040

# 4. YARN Cluster 模式（生产）
spark-submit \
  --master yarn --deploy-mode cluster \
  --num-executors 10 --executor-memory 8g --executor-cores 4 \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.sql.autoBroadcastJoinThreshold=50MB \
  execution-demo.py

# 5. History Server（需配置 eventLog）
# spark.eventLog.enabled=true
# spark.eventLog.dir=hdfs:///spark-logs
$SPARK_HOME/sbin/start-history-server.sh
open http://localhost:18080
```

---

## 与其他章节的关系

| 章节 | 本章铺垫 / 衔接 |
|------|----------------|
| D22 Spark 概览与定位 | 架构三角、RDD/DF/Dataset → 本章展开执行细节 |
| D24 Spark SQL 实操 | DataFrame API + Catalyst → 本章讲 Catalyst 优化后的物理执行 |
| D25 Spark 调优 | Shuffle 是瓶颈 → 本章讲"为什么"，D25 讲"怎么调" |
| D27 流批一体叙事 | Structured Streaming 微批本质 → 本章理解微批内部的 Stage 划分 |
| Flink Checkpoint 指南 | Spark Lineage vs Flink Checkpoint → 本章 Step 5 加分点对比 |
