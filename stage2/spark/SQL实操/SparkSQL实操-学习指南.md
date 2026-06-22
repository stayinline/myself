# Spark SQL 实操 — 学习指南（D24 · L2）

> 配套练习：端到端批处理 ETL（读→清洗→聚合→落表），DataFrame API + Spark SQL 双写法  
> 前置知识：D22 Spark 概览（RDD/DF/Dataset 抽象）、D23 执行原理（Stage/Shuffle）  
> 深度目标：能独立写一个批处理 ETL（读→清洗→聚合→落表）

---

## 读前扫盲：Spark SQL 是「声明式 ETL」，你写意图、Catalyst 管执行

很多人把 Spark SQL 等同于"在 Spark 里写 SQL"，这只是表面。本质是：**你用 DataFrame API 或 SQL 描述"要什么"，Catalyst 优化器自动决定"怎么做"**——谓词下推、列裁剪、Join 选择、Whole-Stage CodeGen 全部自动完成。

入门先记住三个结论：

| 结论 | 为什么重要 |
|------|------------|
| DataFrame API 和 Spark SQL **性能完全等价**，都走 Catalyst 优化链 | 选型不看性能，看工程可读性和团队习惯 |
| Catalyst 四阶段（Parse→Analyze→Optimize→Physical）是理解一切优化的钥匙 | 面试加分项：能讲谓词下推、列裁剪、Code Generation |
| 生产 ETL 必须显式指定 Schema + 动态分区写入 + 小文件治理 | 三大陷阱，踩任何一个都是线上事故 |

本指南用一个完整的 **订单日报 ETL** 串起全流程：CSV/Hive 读取 → 清洗去重 → 维表关联 → 多维聚合 → Parquet/Hive 写出，并用 DataFrame API 和 Spark SQL 各做一遍。

---

## Step 1 原理：DataFrame/Dataset API、Catalyst 优化器与 Hive 集成

### ① DataFrame = Dataset[Row]，核心 API 三类

```
┌─────────────────────────────────────────────────┐
│          DataFrame / Dataset API 分类             │
├──────────────┬──────────────────┬───────────────┤
│ Transformation│     Action       │    DDL        │
│ (惰性,不触发) │ (触发 Job 执行)   │ (部分立即生效) │
├──────────────┼──────────────────┼───────────────┤
│ select       │ show / collect   │ createOrReplace│
│ filter/where │ count / take     │ TempView      │
│ groupBy/agg  │ write / foreach  │ cache/persist │
│ join         │ describe         │ repartition   │
│ withColumn   │ explain          │               │
└──────────────┴──────────────────┴───────────────┘
```

**Column 表达式速查**：`F.col("amount")` 引用列；`F.when(...).otherwise(...)` 条件分支；`F.count/sum/avg/countDistinct` 聚合；`F.row_number().over(Window...)` 窗口函数。⚠️ UDF 尽量避免——Catalyst 无法穿透优化，优先用内置函数。

> **记忆口诀**：**Trans 惰、Act 触、DDL 半即时** — 三类操作节奏不同。

### ② Catalyst 优化器四阶段

这是 Spark SQL 高性能的核心引擎，也是面试高频考点：

```
  SQL / DataFrame API
         │ parse
         ▼
  ┌─────────────────┐
  │ Unresolved       │ ← AST，表/列只是字符串
  │ Logical Plan     │
  └────────┬────────┘
           │ analysis (Catalog 绑定)
           ▼
  ┌─────────────────┐
  │ Analyzed         │ ← 验证表存在、列类型匹配
  │ Logical Plan     │
  └────────┬────────┘
           │ optimization rules
           ▼
  ┌─────────────────┐
  │ Optimized        │ ← ★ 谓词下推、列裁剪、常量折叠
  │ Logical Plan     │   Join Reorder、NULL 传播...
  └────────┬────────┘
           │ physical planning + CBO
           ▼
  ┌─────────────────┐
  │ Selected         │ ← 最终物理计划
  │ Physical Plan    │   → Whole-Stage CodeGen → Java 字节码
  └─────────────────┘
```

**核心优化规则**：

| 优化规则 | 说明 | 效果示例 |
|---------|------|---------|
| **谓词下推** | filter 推到离数据源最近的位置 | Scan 时直接带 WHERE，非全表扫描后过滤 |
| **列裁剪** | 只读 SELECT 中用到的列 | `SELECT name,age` 不读 city/email/phone |
| **常量折叠** | 编译期计算常量表达式 | `1+2+col` → `3+col` |
| **Join Reorder** | CBO 调整 Join 顺序 | 小表自动 Broadcast |
| **Whole-Stage CodeGen** | 多算子融合为一个 Java 方法 | scan+filter+project 无中间物化 |

```
优化前:                                优化后:
Project [name, age]                    Project [name, age]
  Filter (age > 25)                      Filter (age > 25)    ← 下推到 Scan
    Scan [name,age,city,email,phone]       Scan [name, age]   ← 列裁剪
```

用 `df.explain(mode="formatted")` 可查看完整优化过程。

> **面试话术**：Catalyst 四阶段优化是 Spark SQL 比 RDD 快 2~10 倍的根本原因。开发者写声明式查询，优化器自动做谓词下推、列裁剪、Join 选择和 CodeGen。RDD 绕过整个 Catalyst 链，所以同样逻辑手写 RDD 既慢又容易出错。

### ③ Hive 集成与分区裁剪

**Spark on Hive**（主流）：Spark 作为执行引擎，通过 `enableHiveSupport()` 读取 Hive Metastore 元数据，直接 `spark.sql("SELECT ... FROM hive_table")`。Hive on Spark 已废弃。

```python
spark = SparkSession.builder \
    .enableHiveSupport() \                                    # ★ 关键
    .config("hive.metastore.uris", "thrift://meta-host:9083") \
    .getOrCreate()
```

**分区裁剪（Partition Pruning）**——最重要的查询优化之一：

```python
# ✅ 触发分区裁剪：只扫描 dt=2024-06-01 的文件
df.filter(F.col("dt") == "2024-06-01")
# ❌ 不触发：函数包裹了分区列
df.filter(F.substring(F.col("dt"), 1, 7) == "2024-06")
# ✅ 改写为范围过滤即可触发
df.filter((F.col("dt") >= "2024-06-01") & (F.col("dt") <= "2024-06-30"))
```

---

## Step 2 实操：端到端批 ETL（DataFrame API + Spark SQL 双写法）

### 场景：在线教育订单日报

```
┌──────────┐    ┌──────────┐    ┌──────────────┐    ┌──────────┐
│ CSV      │    │ Hive/    │    │ Spark ETL    │    │ Parquet  │
│ 原始订单  │───►│ Parquet  │───►│ 清洗→关联→聚合│───►│ Hive DWS │
│ raw_orders│    │ dim_user │    │              │    │ 日报汇总  │
└──────────┘    └──────────┘    └──────────────┘    └──────────┘
```

### 准备测试数据

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("etl-demo").master("local[4]") \
    .enableHiveSupport() \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# === 生成模拟 CSV（10 万条订单）===
import os, csv, random
os.makedirs("/tmp/etl_demo/raw_orders", exist_ok=True)
with open("/tmp/etl_demo/raw_orders/orders.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["order_id","user_id","amount","status","order_date"])
    for i in range(100000):
        writer.writerow([i, random.randint(1,500), round(random.uniform(10,2000),2),
            random.choice(["paid","cancelled","refunded","pending"]),
            f"2024-{random.randint(1,6):02d}-{random.randint(1,28):02d}"])

# === 创建用户维表 ===
users_df = spark.createDataFrame(
    [(uid, f"user_{uid}", random.choice(["北京","上海","广州","深圳"]))
     for uid in range(1, 501)], ["user_id","user_name","city"])
users_df.write.mode("overwrite").parquet("/tmp/etl_demo/dim_user")
```

### 写法一：DataFrame API（工程化推荐）

```python
# ========== 读取（显式 Schema！）==========
order_schema = StructType([
    StructField("order_id", LongType()), StructField("user_id", LongType()),
    StructField("amount", DecimalType(10,2)), StructField("status", StringType()),
    StructField("order_date", DateType())])
raw_orders = spark.read.schema(order_schema).option("header","true").csv("/tmp/etl_demo/raw_orders/")
dim_user = spark.read.parquet("/tmp/etl_demo/dim_user")

# ========== 清洗 ==========
cleaned = (raw_orders
    .filter(F.col("order_id").isNotNull()).filter(F.col("amount") > 0)
    .filter(F.col("status").isin("paid","pending"))
    .dropDuplicates(["order_id"]).withColumn("etl_time", F.current_timestamp()))

# ========== 关联维表 + 聚合 ==========
summary = (cleaned.join(dim_user, on="user_id", how="left")
    .withColumn("city", F.coalesce(F.col("city"), F.lit("unknown")))
    .groupBy("order_date","city").agg(
        F.count("*").alias("order_cnt"), F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"), F.countDistinct("user_id").alias("distinct_users"))
    .orderBy("order_date","city"))

# ========== 写出（按日期分区）==========
summary.write.mode("overwrite").partitionBy("order_date") \
    .option("compression","snappy").parquet("/tmp/etl_demo/dws_daily_summary")
print("✅ DataFrame API ETL 完成!")
```

### 写法二：Spark SQL（Ad-hoc / BI 对接推荐）

```python
cleaned.createOrReplaceTempView("cleaned_orders")
dim_user.createOrReplaceTempView("dim_user")

result_sql = spark.sql("""
    SELECT o.order_date, COALESCE(u.city,'unknown') AS city,
        COUNT(*) AS order_cnt, SUM(o.amount) AS total_amount,
        AVG(o.amount) AS avg_amount, COUNT(DISTINCT o.user_id) AS distinct_users
    FROM cleaned_orders o LEFT JOIN dim_user u ON o.user_id = u.user_id
    GROUP BY o.order_date, COALESCE(u.city,'unknown')
    ORDER BY o.order_date, city""")

# 建 Hive 表 + 动态分区写入
spark.sql("CREATE TABLE IF NOT EXISTS dws_daily_summary_hive USING parquet PARTITIONED BY (order_date) LOCATION '/tmp/etl_demo/dws_daily_summary_hive'")
result_sql.createOrReplaceTempView("dws_result")
spark.sql("INSERT OVERWRITE TABLE dws_daily_summary_hive SELECT city,order_cnt,total_amount,avg_amount,distinct_users,order_date FROM dws_result")
print("✅ Spark SQL ETL 完成!")
```

### 两种写法对比

| 维度 | DataFrame API | Spark SQL |
|------|--------------|-----------|
| 可读性 | 链式调用，IDE 补全好 | 类传统 SQL，分析师友好 |
| 复用性 | 容易封装函数/类 | 字符串拼接较麻烦 |
| 调试 | 可逐步 show/printSchema | 需 explain 或临时 view |
| **性能** | **完全相同**（都走 Catalyst） | **完全相同** |
| 推荐场景 | **工程化 ETL 管道** | **Ad-hoc 查询、BI 对接** |

> **面试话术**：DataFrame API 和 Spark SQL 性能完全等价，都经过 Catalyst 优化器。工程化 ETL 我倾向 DataFrame API——有 IDE 补全、单元测试方便、易于封装。Ad-hoc 分析用 SQL 更直接。实际项目中两者经常混用。

---

## Step 3 对比：Spark SQL 与 Flink SQL 批处理体验差异

| 维度 | Spark SQL | Flink SQL | 选型启示 |
|------|-----------|-----------|---------|
| Hive 兼容 | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | 数仓 ETL 首选 Spark |
| CBO + CodeGen | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | 大查询性能 Spark 领先 |
| Connector 丰富度 | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | CDC/Kafka Flink 更强 |
| 交互式查询 | ⭐⭐⭐⭐ | ⭐⭐ | Ad-hoc 分析选 Spark |
| 流批统一语法 | ⭐⭐ | ⭐⭐⭐⭐⭐ | 一套代码兼顾选 Flink |
| 增量批处理 | ⭐⭐ | ⭐⭐⭐⭐ | Paimon 增量读选 Flink |

**语法差异**：DDL 上 Spark 用 `USING parquet`，Flink 用 `WITH ('connector'=...)`；窗口上 Flink 多了 TVF（TUMBLE/HOP/CUMULATE）；Catalog 上 Flink 支持 Paimon。

> **面试话术**：纯批处理场景 Spark SQL 仍是首选——Hive 兼容性最好、CBO 最成熟、执行效率最高。Flink SQL 的优势在于同一套 SQL 既能跑批又能跑流。现在的趋势是用湖表（Paimon/Iceberg）做中间层，Spark 和 Flink 都能读写同一张表，既保留各自执行优势，又在存储层实现统一。

---

## Step 4 陷阱：小文件、Schema 推断、分区写入

### ① 小文件问题（写出分区过多）

```
/user/hive/warehouse/dws_table/dt=2024-06-01/
├── part-00000.snappy.parquet   (1KB)   ← 太小!
├── part-00001.snappy.parquet   (2KB)
...
└── part-00199.snappy.parquet   (3KB)   ← 200 个小文件
```

**危害**：NameNode 元数据压力（每文件 ~150B 内存）+ 大量 Task 启动开销 + HDFS 块利用率低。

**产生原因**：shuffle partitions 默认 200 / 动态分区 × Task / 流式 micro-batch / 频繁 INSERT INTO。

**解决方案**：

```python
# 方案一：写入前合并分区
df.coalesce(10).write.parquet("output")
# 方案二：调 shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "20")
# 方案三：AQE 自动合并（Spark 3.x 推荐 ★）
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64MB")
```

**AQE 原理**：检测到某些分区输出 < minPartitionSize → 合并多个小分区 → 下游 Task 数减少 → 输出文件从 200 个变为 ~30 个合理大小文件。

### ② Schema 推断陷阱

```python
# ❌ 生产慎用！扫描全量数据推断类型，大文件很慢且可能不准
df = spark.read.option("inferSchema", "true").csv("data/")
# ✅ 推荐：显式指定 schema
schema = StructType([StructField("order_id", LongType()), StructField("amount", DecimalType(10,2))])
df = spark.read.schema(schema).csv("data/")
```

### ③ 分区写入陷阱

```python
# ⚠️ 不设 dynamic → INSERT OVERWRITE 删除整个表再重写！
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")  # ✅ 必设
```

| 写入模式 | 行为 | 适用场景 |
|---------|------|---------|
| `overwrite` | 覆盖目标（全部或动态分区） | 全量刷新 |
| `append` | 追加新文件 | 增量写入 |
| `ignore` | 目标存在则跳过 | 幂等初始化 |
| `errorIfExists` | 目标存在则报错 | 安全检查 |

> **记忆口诀**：**小文靠 AQE，Schema 要显式，分区设 dynamic** — 三大陷阱三字诀。

---

## Step 5 面试话术

### 问：你用 Spark 做过什么批处理？

> 我做过一个在线教育平台的 **订单日报 ETL**。每天凌晨从 Hive ODS 层读取前一天的原始订单（约 500 万条），和用户维表做 Left Join 补全城市信息，然后按「日期×城市」两个维度聚合出订单数、GMV、客单价、去重用户数四个指标，结果写入 DWS 层的 Parquet 分区表供 BI 报表消费。
>
> 技术要点有三个：第一，**显式指定 Schema** 而非 inferSchema，避免全量扫描和类型误判；第二，开启 **AQE 自动合并小文件**，防止动态分区写入产生数千个小文件拖垮 NameNode；第三，设置 **partitionOverwriteMode=dynamic**，确保只覆盖当天分区而非整表重写。
>
> 这个 Job 用 DataFrame API 写主流程（便于单元测试和封装），复杂 Ad-hoc 排查用 Spark SQL 临时查。两种方式性能完全一样，都走 Catalyst 优化链。

### 追问：Catalyst 做了哪些优化你能举例吗？

> 最典型的是 **谓词下推** 和 **列裁剪**。比如 `df.select("name","age").filter(col("age")>25)`，Catalyst 自动把 filter 推到 Scan 之前，并且只读 name 和 age 两列。另外 **Whole-Stage CodeGen** 把 scan+filter+project 编译成一个 Java 方法，消除算子间序列化和虚函数调用。这些优化手写 RDD 做不到，也是推荐用 DataFrame/SQL 的根本原因。

### 追问：Spark SQL 和 Flink SQL 做批处理怎么选？

> 纯离线 ETL 选 Spark SQL——Hive 兼容性最好、CBO 最成熟、社区资料最丰富。Flink SQL 优势是流批一体语法，同时有实时和离线需求可减少维护成本。我们用 Paimon 湖表做中间层，Spark 离线分析、Flink 实时写入，在存储层统一。

---

## Step 6 在线教育业务案例（ETL 三角）

> 以下三个场景是在线教育平台里 **Spark SQL 批处理最高频** 的业务，分别对应：  
> **T+1 报表 ETL** / **数据质量清洗** / **跨域关联分析**

### 案例一：T+1 学员学习日报 → 标准 ETL

教务系统每天凌晨汇总学员前一天的完课数、学习时长、答题正确率，生成班主任可见的「每日学情看板」。数据量 300 万条/天。

```
Hive(dwd_study_record) ──┐
                          ├── Spark SQL 聚合 ──→ Hive(dws_daily_study) ──→ BI
Hive(dim_student)       ──┘
```

#### 三要素

| 维度 | 分析 |
|------|------|
| 根因需求 | T+1 时效、多维聚合、Hive 生态 |
| 技术选型 | Spark SQL + Catalyst 自动优化，吞吐高 |
| 关键配置 | AQE 合并小文件 + 动态分区 + 显式 Schema |

**踩坑**：未设 `partitionOverwriteMode=dynamic` 导致重跑清空历史分区；`shuffle.partitions` 默认 200 产出小文件告警。

---

### 案例二：原始日志清洗 → 数据质量关卡

App 端上报的学习心跳日志格式不规范：字段缺失、时间戳异常、重复上报。需在 DWD 层入口做清洗。

```
Hive(ods_app_heartbeat) → Spark DataFrame(filter+dedup+fillna+withColumn) → Hive(dwd_heartbeat_clean)
```

#### 三要素

| 维度 | 分析 |
|------|------|
| 根因需求 | 脏数据不能流入下游，清洗规则多变 |
| 技术选型 | DataFrame API 链式调用，规则易扩展、可单测 |
| 关键配置 | 缓存中间结果避免重复计算；清洗规则抽取为函数 |

**踩坑**：Python UDF 做时间解析性能差 10 倍，改用内置 `to_timestamp()` 提速明显；未对清洗前后做 count 比对，上线后过滤过严丢了 30% 数据。

---

### 案例三：课程销售漏斗分析 → 跨域 Join

运营需要「曝光→点击→试听→付费」转化漏斗，涉及埋点表、订单表、课程表三张大表 Join。

```
Hive(dwd_page_view) + Hive(dwd_order) + Hive(dim_course) → Spark SQL Join+Window → Hive(dws_funnel)
```

#### 三要素

| 维度 | 分析 |
|------|------|
| 根因需求 | 多表关联、窗口排名、大数据量 |
| 技术选型 | Spark SQL CBO 自动选 Join 策略；Broadcast 小表 |
| 关键配置 | `autoBroadcastJoinThreshold` 调大到 100MB；ANALYZE TABLE 收集统计 |

**踩坑**：课程维表 80MB 超默认 10MB Broadcast 阈值，走 SortMergeJoin 耗时 40min，调大后 8min 完成；未在分区列上过滤就 Join 导致全表扫描。

---

### 三案例对照总表

| 案例 | ETL 主题 | 核心技术 | 关键配置 | 常见踩坑 |
|------|---------|---------|---------|---------|
| 学员学习日报 | 标准聚合 ETL | groupBy + agg | AQE + 动态分区 | 小文件、分区覆盖 |
| 日志清洗 | 数据质量 | filter + dedup + fillna | 内置函数替代 UDF | UDF 性能、过滤过严 |
| 销售漏斗 | 多表 Join | Join + Window | Broadcast 阈值 + CBO | Join 策略、全表扫描 |

---

## 验收清单

| # | 验收项 | 验证方式 |
|---|--------|----------|
| ① | 能独立跑通端到端批 ETL（读→清洗→聚合→写出） | Step 2 完整代码 |
| ② | 能用 DataFrame API 和 Spark SQL 各写一遍同一逻辑 | Step 2 双写法对比 |
| ③ | 能讲 Catalyst 四阶段 + 谓词下推/列裁剪 | Step 1② + explain 验证 |
| ④ | 能解决小文件问题（AQE/coalesce/repartition） | Step 4① + 实操验证 |
| ⑤ | 知道 Schema 推断风险和动态分区写入陷阱 | Step 4②③ |
| ⑥ | 能讲 Spark SQL vs Flink SQL 批处理差异 | Step 3 对比表 |
| ⑦ | 能结合 demo 讲"你用 Spark 做过什么批处理" | Step 5 面试话术 |

---

## 运行速查

```bash
# 1. 启动 PySpark REPL（本地调试）
pyspark --master local[4] --conf spark.sql.shuffle.partitions=8

# 2. 提交 ETL 作业（本地）
spark-submit --master local[4] --driver-memory 2g etl_template.py

# 3. 提交 ETL 作业（YARN Cluster，生产）
spark-submit \
  --master yarn --deploy-mode cluster \
  --num-executors 10 --executor-memory 8g --executor-cores 4 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.sources.partitionOverwriteMode=dynamic \
  etl_template.py

# 4. Spark UI
open http://localhost:4040     # Jobs/Stages/SQL Tab

# 5. 查看执行计划
# df.explain(mode="formatted")
```

### ETL 脚本模板速查

```python
def create_spark_session(app_name):
    return (SparkSession.builder.appName(app_name).enableHiveSupport()
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", "200").getOrCreate())

def extract(spark):
    return {"orders": spark.read.parquet("hdfs:///data/dwd/orders/"),
            "users":  spark.read.parquet("hdfs:///data/dim/users/")}

def transform(data):
    return (data["orders"].filter(F.col("status")=="paid")
        .join(data["users"], on="user_id", how="left")
        .groupBy("order_date","city")
        .agg(F.count("*").alias("cnt"), F.sum("amount").alias("gmv")))

def load(df, path):
    df.write.mode("overwrite").partitionBy("order_date") \
      .option("compression","snappy").parquet(path)
```

---

## 与其他章节的关系

| 章节 | 本章铺垫 / 衔接 |
|------|----------------|
| D22 Spark 概览 | RDD→DF→Dataset 抽象演进 → 本章深入 DF/SQL API |
| D23 执行原理 | Stage/Shuffle 机制 → 本章 ETL 中的 Shuffle 边界、AQE |
| D25 Spark 调优 | 本章提到的小文件/AQE/Broadcast → 展开讲倾斜、内存、GC |
| D27 流批一体叙事 | 本章 Step 3 Spark vs Flink → Lambda→Kappa→湖仓演进 |
| D41 流批一体落地 | 本章 Hive 集成 + 湖表读写 → 端到端架构设计 |

---

## 记忆口诀

```
Trans 惰、Act 触、DDL 半即时
Catalyst 四段：解、析、优、物
小文靠 AQE，Schema 要显式，分区设 dynamic
批选 Spark、流选 Flink、湖表统一两手抓
```
