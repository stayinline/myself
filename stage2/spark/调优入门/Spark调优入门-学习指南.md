# Spark 调优入门 — 学习指南（D25 · L2）

> 配套代码：倾斜 Join 构造 + Broadcast / Salting / AQE 对比实验  
> 前置知识：Flink 两阶段聚合（你已掌握）、Spark 执行原理（D23）  
> 对标：Flink Checkpoint 指南同一风格，由浅入深

---

## 读前扫盲：Spark 调优的本质是「消除长尾 + 减少无效 Shuffle」

Spark 作业慢，80% 的原因归结为两类：**数据倾斜**（个别 Task 处理 90% 的数据）和 **Shuffle 过多**（不必要的网络传输）。调优不是背参数，而是 **定位瓶颈 → 选策略 → 验证效果** 的闭环。

入门先记住三个结论：

| 结论 | 为什么重要 |
|------|------------|
| 数据倾斜用 **加盐/Broadcast/AQE** 三板斧解决 | 这是面试和生产中最高频的调优场景 |
| 内存模型是 **Execution + Storage 动态共享**，不是固定切分 | 盲目调大 executor 内存不一定有效，要分清哪块不够 |
| AQE（Spark 3.x）把很多手动调参变成**运行时自适应** | 默认开启后行为变化必须了解，否则"调了没用"或"没调却变了" |

本指南用 **构造倾斜 Join → 三种优化手段对比 → Flink 映射** 串起完整调优链路，用 **在线教育业务场景** 说明每种手段的落地时机。

---

## Step 1 原理：五大调优手段逐一拆解

### ① 数据倾斜：长尾效应是性能杀手

```
正常分布:                    倾斜分布:
Task0: ████                  Task0: ████████████████████████ ← 热点
Task1: ████                  Task1: ██
Task2: ████                  Task2: █
Task3: ████                  Task3: ██
Task4: ████                  Task4: █

总耗时 ≈ max(Task)           总耗时 = 热点 Task（木桶短板）
```

**根因**：Shuffle 按 key hash 分区，当某些 key 数据量远超其他 key 时，对应分区所在的 Task 成为瓶颈。常见于 `groupBy`、`join`、`distinct` 等操作。

### ② 加盐（Salting）：打散热点 key

**核心思想**：给热点 key 添加随机前缀 → 分散到多个分区做局部聚合 → 去掉前缀做全局聚合。

```
原始数据:                           加盐后 (NUM_SALTS=10):
user_0: ████████████████ (60%)     user_0_0: ████
user_1: █                           user_0_1: ████
user_2: █                           user_0_2: ████
                                    ...
                                    user_0_9: ████
                                    user_1_3: █
                                    user_2_7: █

单 Task 瓶颈                       均匀分散到 10 个 Task
```

**适用条件**：
- ✅ 聚合操作（groupBy + agg）
- ✅ 热点 key 可枚举或可识别
- ❌ Join 操作优先用 Broadcast
- ❌ 全表均匀倾斜（没有明显热点）

### ③ Broadcast Join：小表广播消除 Shuffle

```
Shuffle Join:                          Broadcast Join:
┌──────────┐    Shuffle    ┌────────┐  ┌──────────┐  Broadcast  ┌────────┐
│ Orders   │══════════════►│ Join   │  │ Orders   │────────────►│ Join   │
│ (大表)    │               │ Task   │  │ (大表)    │  (无需shuffle)│ Task   │
└──────────┘               └────────┘  └──────────┘             └────────┘
┌──────────┐    Shuffle         ▲       ┌──────────┐                 ▲
│ Users    │════════════════════┘       │ Users    │ 广播到每个Executor│
│ (小表)    │                            │ (小表)    │ 内存中Hash Table │
└──────────┘                            └──────────┘
```

| 维度 | Shuffle Join | Broadcast Join |
|------|-------------|----------------|
| 网络开销 | 双表都 Shuffle | 仅广播小表 |
| Stage 数 | 多一个 Shuffle Stage | 少一个 Stage |
| 倾斜影响 | 热点 key 拖慢 Task | 无 Shuffle 不受倾斜影响 |
| 限制 | 无 | 小表须放进单节点内存；不支持 full outer join |

### ④ 内存模型：Execution + Storage 动态共享

```
┌─────────────────────────────────────────────────────┐
│                    JVM Heap                          │
│  ┌─────────────────────────────────────────────────┐│
│  │          spark.executor.memory                   ││
│  │  ┌──────────────────┬────────────────────────┐  ││
│  │  │ Execution Memory │   Storage Memory        │  ││
│  │  │ (Shuffle/Join/   │   (Cache/Persist/       │  ││
│  │  │  Sort/Agg Buf)   │    Broadcast/Unroll)    │  ││
│  │  │                  │                         │  ││
│  │  │   ◄── 动态借用 ──►│                         │  ││
│  │  └──────────────────┴────────────────────────┘  ││
│  │          ↑ memory.fraction = 0.6                ││
│  │  ┌──────────────────────────────────────────┐   ││
│  │  │ User Memory (用户对象, 1-0.6=0.4)         │   ││
│  │  └──────────────────────────────────────────┘   ││
│  │  ┌──────────────────────────────────────────┐   ││
│  │  │ Reserved Memory (固定 300MB)              │   ││
│  │  └──────────────────────────────────────────┘   ││
│  └─────────────────────────────────────────────────┘│
│  Off-Heap: spark.memory.offHeap.size (Tungsten缓冲) │
│  Overhead: spark.executor.memoryOverhead (Metaspace) │
└─────────────────────────────────────────────────────┘
```

**关键点**：Execution 和 Storage 之间可以**动态借用**。当 Shuffle Buffer 不够时，会向 Storage 借内存；反之 Cache 也可以借用 Execution 的空闲空间。但借用有底线——Storage 至少保留 `storageFraction`(默认 0.5) 的比例。

### ⑤ 分区数与 AQE

**spark.sql.shuffle.partitions**：控制 Shuffle 后的分区数，直接影响并行度。

| 数据量 | 推荐值 | 理由 |
|--------|--------|------|
| < 1GB | 10-50 | 避免过多小 Task |
| 1-10GB | 50-200 | 平衡并行度和调度开销 |
| 10-100GB | 200-500 | 充分利用集群 |
| > 100GB | 500-2000 | 每分区 ≈ 128MB |

**AQE（Adaptive Query Execution）三大能力**：

```
┌────────────────────┬───────────────────┬──────────────────┐
│  动态合并小分区      │  自适应 Join 策略  │  倾斜 Join 处理   │
│                    │                   │                  │
│ Shuffle 后发现很多  │ 运行时根据实际数据  │ 检测倾斜分区      │
│ 分区很小 → 合并    │ 大小选择最优 Join  │ 自动拆分为子分区   │
│ 减少 Task 启动开销  │ SortMerge→Broadcst│ 并行处理          │
│                    │ Hash→SortMerge    │ 无需改代码         │
└────────────────────┴───────────────────┴──────────────────┘
```

AQE 工作流：**编译期计划 → 执行 Stage → 收集 Shuffle 统计 → 动态调整后续计划**（合并小分区 / 切换 Join 策略 / 拆分倾斜分区）。

> **记忆口诀**：**盐、播、存、区、适** — 五字对应五大手段：加盐打散、Broadcast 广播、内存模型、分区调优、AQE 自适应。

---

## Step 2 实操：构造倾斜 Join，三种优化对比

### 构造倾斜数据 + 四组对比实验

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time, random

# ── Session（先关 AQE，手动体验）──
spark = SparkSession.builder.appName("tuning-demo").master("local[4]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.sql.adaptive.enabled", "false").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ── 倾斜订单表：user_id=0 占 60% ──
hot = [(i, 0, round(random.uniform(10,500),2)) for i in range(600000)]
norm = [(i+600000, (i%999)+1, round(random.uniform(10,500),2)) for i in range(400000)]
orders = spark.createDataFrame(hot+norm, ["order_id","user_id","amount"]).repartition(8).cache()

# ── 用户维表（小表 1000 行）──
users = spark.createDataFrame([(u, f"user_{u}") for u in range(1000)], ["user_id","user_name"]).cache()

# ═══ 实验 A: Baseline Shuffle Join ═══
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
t0 = time.time(); orders.join(users, "user_id").count(); print(f"Shuffle: {time.time()-t0:.1f}s")

# ═══ 实验 B: Broadcast Join ═══
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")
t0 = time.time(); orders.join(users, "user_id").count(); print(f"Broadcast: {time.time()-t0:.1f}s")

# ═══ 实验 C: Salting 加盐 groupBy ═══
SALTS = 10
salted = orders.withColumn("salt",(F.rand()*SALTS).cast("int")) \
    .withColumn("sk", F.concat_ws("_", F.col("user_id").cast("string"), F.col("salt")))
partial = salted.groupBy("sk").agg(F.sum("amount").alias("ps"), F.count("*").alias("pc"))
final = partial.withColumn("uid", F.split("sk","_")[0].cast("long")) \
    .groupBy("uid").agg(F.sum("ps").alias("total"), F.sum("pc").alias("cnt"))
t0 = time.time(); final.collect(); print(f"Salted: {time.time()-t0:.1f}s")

# ═══ 实验 D: AQE 自动优化 ═══
spark_aqe = SparkSession.builder.appName("aqe").master("local[4]") \
    .config("spark.sql.shuffle.partitions","200") \
    .config("spark.sql.adaptive.enabled","true") \
    .config("spark.sql.adaptive.skewJoin.enabled","true").getOrCreate()
t0 = time.time()
orders.join(users,"user_id").groupBy("user_id").agg(F.sum("amount")).count()
print(f"AQE: {time.time()-t0:.1f}s")
```

### Spark UI 观察路径

**http://localhost:4040** → 重点关注：

| Tab / 指标 | 关注点 | 本 Demo 如何放大 |
|-----------|--------|------------------|
| **Stages → Max Task Duration** | 倾斜程度 = max / median | Baseline 中 user_0 所在 Task 远超其他 |
| **Stages → Shuffle Read/Write** | Shuffle 数据量 | Broadcast 后降为 0 |
| **SQL → Adaptive Query Execution** | AQE 是否生效 | 实验 D 可见分区合并和倾斜拆分标记 |
| **Executors → GC Time** | 内存压力 | 倾斜 Task GC 时间显著高于其他 |

### 预期对比结果

```
┌─────────────────────────────────────────────────────────────────┐
│                    调优实验记录                                   │
├──────────────┬──────────┬──────────┬──────────┬────────────────┤
│ 指标          │ Baseline │ Broadcast│ Salting  │ AQE            │
├──────────────┼──────────┼──────────┼──────────┼────────────────┤
│ 总耗时(s)     │ ~8.5     │ ~3.2     │ ~3.1     │ ~4.0           │
│ Stage 数      │ 3        │ 2        │ 4        │ 3(动态调整)     │
│ Max Task(s)  │ ~6.0     │ ~2.5     │ ~1.2     │ ~1.5           │
│ Shuffle Read │ 大        │ 0        │ 中等     │ 自动优化        │
│ 倾斜缓解      │ ❌       │ ✅       │ ✅✅     │ ✅              │
└──────────────┴──────────┴──────────┴──────────┴────────────────┘
```

---

## Step 3 对比：Spark 倾斜治理 vs Flink 倾斜治理

你已经熟悉 Flink 的两阶段聚合，现在把它映射到 Spark：

### ① 核心思想一致：分而治之

```
Flink 两阶段聚合:                        Spark Salting:
Phase 1: keyBy(key+"_"+rand)            Phase 1: groupBy(salted_key)
         ↓ window + aggregate                    ↓ agg(partial)
Phase 2: keyBy(originalKey)             Phase 2: groupBy(original_key)
         ↓ window + finalAggregate               ↓ agg(final)
         
流式增量计算                              批式全量计算
状态存在 StateBackend                     中间结果走 Shuffle
```

### ② 详细对比表

| 维度 | Flink 两阶段聚合 | Spark Salting |
|------|-----------------|---------------|
| **触发方式** | 开发者手动实现 | 手动 或 AQE 自动 |
| **窗口对齐** | 需保证两阶段窗口一致 | 不涉及窗口概念 |
| **状态管理** | 每阶段独立 KeyedState | 中间结果通过 Shuffle 传递 |
| **增量 vs 全量** | 增量计算 | 全量批次计算 |
| **自动化** | 无内置自动解倾斜 | AQE 可自动检测和处理 |
| **复杂度** | 较高（窗口语义 + 状态） | 较低（纯数据处理） |
| **适用引擎** | 流处理 | 批处理 / 微批 |

### ③ 迁移你的 Flink 直觉

| Flink 经验 | Spark 等价物 |
|-----------|-------------|
| keyBy 倾斜 → 两阶段聚合 | groupBy → Salting（或 AQE 自动） |
| rebalance() | repartition(n) / coalesce(n) |
| State TTL | cache + unpersist（手动管理） |
| parallelism | shuffle.partitions |
| 反压监控 | Spark UI Task Duration（看长尾） |

> **面试话术**：Flink 和 Spark 解决数据倾斜的核心思想完全一样——"分而治之"。Flink 叫两阶段聚合，Spark 叫 Salting。本质都是给热点 key 加随机前缀打散到多个并行实例，先做局部聚合再做全局聚合。区别在于 Flink 是流式增量计算需要维护两阶段状态，Spark 是批式全量计算中间结果通过 Shuffle 传递。Spark 3.x 的 AQE 可以自动检测和处理倾斜，比 Flink 的手动两阶段更方便，但在流式场景下 Flink 的方案更精细。

---

## Step 4 陷阱：两个高频踩坑点

### 陷阱一：盲目调大 Executor 内存

```
❌ 错误做法：
  看到 OOM 就加大 spark.executor.memory
  → 如果是 Shuffle 倾斜导致的 OOM，加内存只是推迟崩溃
  → 如果是 User Memory 区域溢出，加 heap 也没用（fraction 限制了可用空间）

✅ 正确思路：
  1. 先看 Spark UI → Executors Tab → 哪个 Task OOM
  2. 看 GC Time 占比 → 高则说明内存紧张
  3. 看 Shuffle Spill → 有 spill 说明 Execution Memory 不够
  4. 区分 OOM 类型：
     - Java heap space → 增大 executor.memory 或增加分区数
     - Metaspace → 增大 extraJavaOptions MaxMetaspaceSize
     - Container killed → 增大 memoryOverhead
     - GC overhead → 增大内存 + G1GC + 增加分区
```

### 陷阱二：AQE 默认开启后的行为变化

Spark 3.2+ AQE 默认开启，带来三个容易忽视的变化：

| 变化 | 说明 | 踩坑场景 |
|------|------|---------|
| **shuffle.partitions 被覆盖** | AQE 会自动合并小分区，你设的 200 可能最终只有 20 | 写了 `repartition(200)` 想控制输出文件数，结果被 AQE 合并成少量大文件 |
| **Join 策略被切换** | AQE 运行时发现表比预估小，自动从 SortMerge 切到 Broadcast | 你以为走了 SortMerge 做了 salting 优化，结果 AQE 切了 Broadcast，salting 白做 |
| **倾斜自动处理** | AQE 检测到倾斜分区会自动拆分 | 你手动加了盐，AQE 又拆了一次，反而增加了 Shuffle 开销 |

```
✅ 应对策略：
  1. 生产环境保持 AQE 开启（利大于弊）
  2. 写文件前需要精确控制分区数 → 临时关闭 AQE 或用 coalesce
  3. 手动优化前先确认 AQE 是否已经解决了问题
  4. 看 Spark UI SQL Tab 中的 AQE 标记，确认实际执行计划
```

### 其他陷阱速查

| 陷阱 | 正确做法 |
|------|---------|
| Python UDF 阻止 Catalyst 优化 | 优先用内置函数 |
| collect() 拉爆 Driver | 用 take/show/write |
| cache 不 unpersist | 用完立即释放 |
| 序列化用 Java 默认 | 生产必设 Kryo |

---

## Step 5 面试话术

### 问：Spark 作业很慢 / 数据倾斜，你怎么调？

> 我的调优思路是 **"一定位、二选型、三验证"**。
>
> **第一步，定位瓶颈**。打开 Spark UI，看 Stages 页的 Task Duration 分布。如果 max 远大于 median，就是数据倾斜；如果所有 Task 都慢且 Shuffle Read/Write 很大，是 Shuffle 过多；如果 GC Time 占比高，是内存不足。
>
> **第二步，选策略**。  
> ① **大小表 Join** → 首选 Broadcast Join，把小表广播到所有 Executor，消除 Shuffle。阈值通过 `autoBroadcastJoinThreshold` 控制，生产可以适当提到 50-100MB。  
> ② **GroupBy 倾斜** → 用 Salting 加盐打散热点 key，先局部聚合再全局聚合。这和 Flink 的两阶段聚合思想完全一致。  
> ③ **Join 倾斜** → Spark 3.x 开 AQE，它会自动检测倾斜分区并拆分处理，不需要改代码。  
> ④ **小文件问题** → AQE 的动态分区合并 + 写出前 coalesce。  
> ⑤ **内存/GC** → 分清是 Execution Memory 还是 Storage Memory 不够，针对性调整。盲目加大 executor memory 不一定有效。
>
> **第三步，验证效果**。在 Spark UI 对比优化前后的 Max Task Duration、Shuffle 数据量、GC Time。我通常会记录一张对比表来量化收益。
>
> 最后补充一点：Spark 3.2+ AQE 默认开启，很多之前需要手动调的问题现在自动解决了。但手动优化仍然有价值——已知的小表直接 broadcast 比等 AQE 判断更可靠，确定的热点 key 手动 salting 比 AQE 通用拆分更精准。AQE 是兜底保障，不是万能药。

### 追问：Salting 和 Broadcast 怎么选？

> 看操作类型和数据特征。**Join 场景**，如果一侧是小表（< 几百 MB），优先 Broadcast，因为它直接消除 Shuffle，效果最好且不改业务逻辑。**GroupBy 聚合场景**，或者 Join 两侧都是大表时，用 Salting 打散热点 key。两者也可以组合——先 Broadcast 小表完成 Join，再对结果做 Salting 聚合。

### 追问：AQE 开了之后还需要手动调优吗？

> 需要。AQE 解决的是"编译期无法准确预估运行时数据分布"的问题。对于已知的、固定的数据特征，手动调优更精确。比如你知道某张表一定小于 100MB，直接手动 broadcast 比等 AQE 判断更可靠。AQE 的价值在于兜底——当你遗漏了某些倾斜场景，或者数据分布随时间变化时，AQE 能自动补上。

---

## Step 6 在线教育业务案例（调优三角）

> 以下三个场景是在线教育平台里 **Spark 调优最高发** 的业务，分别对应：  
> **倾斜 Join → Broadcast** / **热点聚合 → Salting** / **通用优化 → AQE**

---

### 案例一：学员订单 Join 用户维表 — Broadcast 消除 Shuffle

#### 业务背景

每日凌晨 ETL：订单事实表（5000 万行）Join 用户维表（50 万行）生成宽表。用户维表仅 80MB，但默认 threshold=10MB 未触发 Broadcast，走了 SortMerge Join。

#### 故障现象

```
Shuffle Write 12GB → Shuffle Read 12GB → Stage 耗时 25min
其中热点用户（测试账号 user_id=0）的 Task 独占 18min
```

#### 三要素

| 维度 | 分析 |
|------|------|
| 根因 | 小表未触发 Broadcast + 热点 key 倾斜叠加 |
| 信号 | Spark UI Shuffle Read/Write 巨大；Max Task >> Median Task |
| 对策 | `autoBroadcastJoinThreshold` 调到 100MB → 自动 Broadcast；热点测试账号过滤或单独处理 |

#### 踩坑

- 只调大 threshold 不过滤测试账号 → Broadcast 消除了 Shuffle 但热点仍在本地聚合阶段拖慢。
- Broadcast 小表超过 Driver 内存 1/3 → Driver OOM。

---

### 案例二：课程完课统计 GroupBy 倾斜 — Salting 打散

#### 业务背景

统计每门课程的完课人数。热门课程（如"Python 入门"）完课记录占总量 40%，groupBy("course_id") 后该课程对应的 Task 处理时间远超其他。

#### 故障现象

```
Stage 共 200 个 Task，199 个在 2s 内完成，1 个跑了 15min
总耗时 = 15min（木桶效应）
```

#### 三要素

| 维度 | 分析 |
|------|------|
| 根因 | 热点课程 key 导致 Shuffle 分区倾斜 |
| 信号 | Max Task Duration / Median > 10x |
| 对策 | Salting 加盐：NUM_SALTS=20 → 热点课程分散到 20 个 Task 并行聚合 → 再去盐全局聚合 |

#### 与 Flink 映射

对应 Flink 两阶段聚合（详见 Step 3）。

---

### 案例三：日报 ETL 通用优化 — AQE 兜底

#### 业务背景

日报 ETL 涉及 20+ 张表的 Join 和聚合，数据量随运营活动波动大。平时数据均匀，大促期间某些品类数据暴增 10 倍，手动调参跟不上变化。

#### 故障现象

```
平日 ETL 40min 跑完，大促期间跑到 3h
手动调 shuffle.partitions 和 broadcast threshold 只能覆盖已知场景
新出现的倾斜来不及排查
```

#### 三要素

| 维度 | 分析 |
|------|------|
| 根因 | 数据分布动态变化，静态配置无法适应 |
| 信号 | 不同日期 Stage 耗时差异大；倾斜 key 不固定 |
| 对策 | 开启 AQE 三大能力全部打开 → 运行时自适应分区合并、Join 策略切换、倾斜拆分 |

#### 踩坑

- AQE 开启后 `shuffle.partitions=500` 可能被合并到 30 → 见 Step 4 陷阱二。

---

### 三案例对照总表

| 案例 | 调优主题 | 典型症状 | 核心对策 | 与 Flink 映射 |
|------|---------|----------|----------|--------------|
| 订单 Join 维表 | **Broadcast** | Shuffle 大 + 倾斜叠加 | 调大 Broadcast threshold | 无直接对应（Flink 无 Broadcast Join） |
| 课程完课统计 | **Salting** | Max Task >> Median | 加盐两阶段聚合 | Flink 两阶段聚合（完全对应） |
| 日报 ETL 波动 | **AQE** | 数据分布动态变化 | AQE 运行时自适应 | Flink 无等价物（流式不需分区调优） |

---

## 验收清单

| # | 验收项 | 验证方式 |
|---|--------|----------|
| ① | 能讲 ≥4 种调优手段及适用场景 | Step 1 五大手段 + Step 5 面试话术 |
| ② | 能演示一次完整的倾斜优化（构造→优化→对比） | Step 2 四组实验 + Spark UI 截图 |
| ③ | 能把 Flink 两阶段聚合映射到 Spark Salting | Step 3 对比表 + 案例二 |
| ④ | 能讲清 AQE 默认开启后的三个行为变化 | Step 4 陷阱二 |
| ⑤ | 能区分 Execution Memory 和 Storage Memory | Step 1④ 内存模型图 |
| ⑥ | 能讲三个业务案例的调优逻辑 | Step 6 三案例 |
| ⑦ | 能脱稿回答"Spark 作业很慢你怎么调" | Step 5 面试话术 |

---

## 运行速查

```bash
# 1. 启动 PySpark REPL（本地调试）
pyspark --master local[4] --conf spark.ui.port=4040

# 2. 提交调优 Demo
spark-submit --master local[4] --driver-memory 2g tuning_demo.py

# 3. 查看 Spark UI
open http://localhost:4040

# 4. 生产提交（YARN + AQE）
spark-submit \
  --master yarn --deploy-mode cluster \
  --num-executors 20 --executor-memory 8g --executor-cores 4 \
  --conf spark.sql.shuffle.partitions=400 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.skewJoin.enabled=true \
  --conf spark.sql.autoBroadcastJoinThreshold=100MB \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  etl_daily_report.py

# 5. History Server（查看已完成作业）
$SPARK_HOME/sbin/start-history-server.sh
open http://localhost:18080
```

### 关键参数速查

| 参数 | 默认 | 建议 |
|------|------|------|
| `spark.sql.shuffle.partitions` | 200 | 目标每分区 ≈128MB |
| `spark.executor.memory` | 1g | 生产 4-16g |
| `spark.sql.autoBroadcastJoinThreshold` | 10MB | 可提到 50-100MB |
| `spark.serializer` | Java | **生产必设 Kryo** |
| `spark.sql.adaptive.enabled` | true(3.2+) | 保持开启 |
| `spark.speculation` | false | 生产建议开启 |

---

## 与其他章节的关系

| 章节 | 本章铺垫 / 衔接 |
|------|----------------|
| D22 Spark 概览与定位 | Step 3 提到的 Shuffle → 本章展开讲如何优化 |
| D23 Spark 执行原理 | Job/Stage/Task 模型 → 本章基于此定位瓶颈 |
| D24 Spark SQL 实操 | DataFrame API → 本章的调优代码都基于 DF/SQL |
| D27 流批一体叙事 | AQE 是批处理独有优化 → 流批一体中 Spark 侧依赖 AQE |
| Flink Checkpoint 指南 | Flink 两阶段聚合 → Step 3 映射到 Spark Salting |
| Flink State 管理 | Flink State TTL → Spark cache/unpersist 手动管理 |
