# Hive 数仓基础 — 学习指南（D26 · L2）

> 📚 本模块：**学习指南（本文）** · [原理](原理.md) · [实战](实战.md)  
> 配套练习：Spark + enableHiveSupport 建分区表、写入、查询  
> 前置知识：HDFS 基本操作、SQL 基础  
> 对标：Flink Checkpoint / Spark 概览同一风格，由浅入深

---

## 读前扫盲：Hive 不是数据库，是「元数据层 + SQL 翻译器」

很多初学者把 Hive 等同于 MySQL。先记住：Hive **不做存储**（数据在 HDFS/S3），**不做计算**（MR 已被 Spark/Presto 替代），核心价值是提供**统一的元数据管理和 SQL 接口**，让多个引擎共享同一份数据目录。

入门先记住三个结论：

| 结论 | 为什么重要 |
|------|------------|
| Hive Metastore 是大数据生态的"公共目录服务" | Spark/Flink/Presto/Trino 都通过它发现表结构和分区信息，改一处全引擎同步 |
| 分区是目录级隔离，分桶是文件级哈希，互补不互斥 | 分区解决"查哪些数据"，分桶解决"数据怎么组织"，生产中最常见组合使用 |
| Hive 的核心价值已从"计算"转向"元数据+规范" | MapReduce 被淘汰，但 Metastore 和分层规范仍是数仓基石 |

---

## Step 1 原理：Metastore + 表类型 + 分区桶 + 存储格式

### ① Hive 在大数据生态中的位置 + Metastore 架构

```
┌─────────────────────────────────────────────────────────┐
│                     计算引擎层                           │
│   Spark / Flink / Presto / Trino / MapReduce            │
│        ▲              ▲              ▲                   │
│        └──────────────┼──────────────┘                   │
│                       │ Thrift RPC                       │
│              ┌────────▼────────┐                         │
│              │ Hive Metastore  │ ← 共享元数据！           │
│              │ (MySQL/PG后端)   │                         │
│              │                  │                         │
│              │ DBS→TBLS→SDS    │                         │
│              │ COLUMNS_V2      │                         │
│              │ PARTITIONS      │                         │
│              │ TABLE_PARAMS    │                         │
│              └────────┬────────┘                         │
│                       │                                  │
│              ┌────────▼────────┐                         │
│              │  HDFS / S3 / OSS│ ← 实际数据存储           │
│              └─────────────────┘                         │
└─────────────────────────────────────────────────────────┘
```

多引擎连接同一个 Metastore：

```python
# Spark
spark = SparkSession.builder.enableHiveSupport() \
    .config("hive.metastore.uris", "thrift://metastore:9083").getOrCreate()
# Flink: CREATE CATALOG hive_catalog WITH ('type'='hive', ...)
# Presto: connector.name=hive-hadoop2, hive.metastore.uri=thrift://...
```

> **关键认知**：理解 Hive 不只是学一个工具，而是理解整个数仓的基础设施。

### ② 内部表 vs 外部表

| 维度 | 内部表（Managed） | 外部表（External） |
|------|------------------|-------------------|
| **创建语法** | `CREATE TABLE` | `CREATE EXTERNAL TABLE` |
| **DROP 行为** | 删元数据 **+ 删数据** | 只删元数据，**保留数据** |
| **典型用途** | ETL 中间表、临时表 | ODS 原始数据、共享数据 |
| **ACID** | 支持（需 `TBLPROPERTIES('transactional'='true')` 等配置） | 默认不支持（Hive 3+ 可配事务外部表，生产少见） |

```
选择决策树：
ODS 原始数据 → External（防误删）
ETL 中间产物 → Managed（可重建）
需要 ACID    → Managed + Transactional
跨团队共享   → External + 权限管控
```

### ③ 分区 vs 分桶

**分区**（目录级隔离）：按列值将数据存放到不同子目录。

```
/user/hive/warehouse/orders/
├── dt=2024-06-01/    ← 分区目录
│   ├── 000000_0      ← 数据文件
│   └── 000001_0
├── dt=2024-06-02/
└── dt=2024-06-03/
```

**分桶**（文件级组织）：对某列做 hash，分散到固定数量的文件中。

```
/user/hive/warehouse/orders/dt=2024-06-01/
├── 000000_0    ← bucket 0 (user_id % 4 == 0)
├── 000001_0    ← bucket 1
├── 000002_0    ← bucket 2
└── 000003_0    ← bucket 3
```

| 维度 | 分区 | 分桶 |
|------|------|------|
| 组织粒度 | 目录 | 文件 |
| 数量 | 动态增长 | 固定 |
| 裁剪 | ✅ WHERE dt='xxx' | ⚠️ 仅对分桶列等值条件触发桶裁剪 |
| Join 优化 | 无 | Bucket Map Join |
| 适用列 | 低基数（日期、地域） | 高基数（user_id） |
| NameNode 压力 | 分区过多时大 | 无额外压力 |

> **记忆口诀**：**分区切目录，裁剪靠 WHERE；分桶哈希定文件，Join 优化显身手**。

### ④ 存储格式：ORC vs Parquet

```
行式存储（CSV）:                    列式存储（ORC/Parquet）:
┌──────┬──────┬──────┬──────┐      ┌──────────────────────┐
│ id   │ name │ age  │ city │      │ id列: [1,2,3,...,N]  │
│ 1    │ Alice│ 30   │ BJ   │      │ name列:[Alice,Bob,..]│
│ 2    │ Bob  │ 25   │ SH   │      │ age列: [30,25,35,..] │
└──────┴──────┴──────┴──────┘      │ city列:[BJ,SH,GZ,..] │
SELECT age WHERE city='BJ'         └──────────────────────┘
→ 读整行再过滤                      → 只读 age + city 列
```

| 维度 | ORC | Parquet |
|------|-----|---------|
| **生态偏向** | Hive/Spark | Spark/Presto/Flink/Iceberg |
| **索引** | Bloom Filter + Min/Max + Row Index | Min/Max + Bloom Filter |
| **嵌套类型** | 支持但复杂 | Dremel 编码更高效 |
| **社区趋势** | Hive 生态维护 | **更广泛采用（湖表默认）** |

选型：**新项目选 Parquet**（跨引擎兼容 + 湖表默认），纯 Hive 老系统可用 ORC。

---

## Step 2 实操：建分区表、写入、查询、看目录结构

### 环境准备（Spark 内嵌 Hive，最轻量）

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("hive-practice").master("local[4]") \
    .enableHiveSupport() \
    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
    .getOrCreate()
spark.sql("CREATE DATABASE IF NOT EXISTS edu_demo")
spark.sql("USE edu_demo")
```

### 建表三件套

```sql
-- ① 外部表（ODS 层）
CREATE EXTERNAL TABLE ods_student_heartbeat (
    student_id BIGINT, course_id BIGINT,
    action_type STRING, duration_sec INT, event_time TIMESTAMP
) PARTITIONED BY (dt STRING)
STORED AS TEXTFILE LOCATION '/data/warehouse/ods/ods_student_heartbeat';

-- ② 内部表（DWD 层，Parquet）
CREATE TABLE dwd_learning_detail (
    student_id BIGINT, course_id BIGINT,
    action_type STRING, duration_sec INT, event_time TIMESTAMP
) PARTITIONED BY (dt STRING)
STORED AS PARQUET TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- ③ 分桶表（优化 Join）
CREATE TABLE dwd_learning_bucketed (
    student_id BIGINT, course_id BIGINT,
    action_type STRING, duration_sec INT
) PARTITIONED BY (dt STRING)
CLUSTERED BY (student_id) INTO 16 BUCKETS STORED AS ORC;
```

> **注意**：分桶表写入必须通过 `INSERT INTO`，直接 COPY 文件不会自动分桶。

### 写入数据

```sql
-- 静态分区
INSERT OVERWRITE TABLE dwd_learning_detail PARTITION (dt='2024-06-01')
SELECT student_id, course_id, action_type, duration_sec, event_time
FROM ods_student_heartbeat
WHERE dt='2024-06-01' AND duration_sec > 0;

-- 动态分区（⚠️ 分区列必须在 SELECT 最后）
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
INSERT OVERWRITE TABLE dwd_learning_detail PARTITION (dt)
SELECT student_id, course_id, action_type, duration_sec, event_time, dt
FROM ods_student_heartbeat WHERE dt BETWEEN '2024-06-01' AND '2024-06-30';
```

```python
# Spark DataFrame 写入（⚠️ 不设 dynamic 会清空整张表！）
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.mode("overwrite").insertInto("dwd_learning_detail")
```

### 查看目录结构与验证裁剪

```bash
# HDFS 目录
hdfs dfs -ls /data/warehouse/ods/ods_student_heartbeat/
# dt=2024-06-01/  dt=2024-06-02/ ...

# 本地（Spark 内嵌 Hive）
ls /tmp/spark-warehouse/edu_demo.db/dwd_learning_detail/
```

```sql
-- ✅ 触发裁剪
EXPLAIN SELECT * FROM dwd_learning_detail WHERE dt = '2024-06-01';
-- ❌ 不触发（函数包裹分区列）
EXPLAIN SELECT * FROM dwd_learning_detail WHERE substr(dt,1,7) = '2024-06';
-- 修正：WHERE dt >= '2024-06-01' AND dt < '2024-07-01'

-- 元数据 & 统计信息
DESCRIBE FORMATTED dwd_learning_detail;
SHOW PARTITIONS dwd_learning_detail;
ANALYZE TABLE dwd_learning_detail COMPUTE STATISTICS FOR COLUMNS student_id;
```

---

## Step 3 对比：Hive（重、批、稳）vs 现代引擎

### ① 数仓分层与 Hive 角色

```
ADS  ← 报表/Dashboard/API          ← Presto/Spark
DWS  ← 汇总宽表/指标表             ← Spark/Presto
DWD  ← 清洗后事实表                ← Spark
DIM  ← 用户/课程维表               ← Spark/Hive
ODS  ← 原始数据镜像(External)      ← Flink/Sqoop/DataX
```

### ② Hive vs 现代引擎

| 维度 | Hive (MR) | Spark SQL | Presto/Trino | Flink SQL |
|------|-----------|-----------|-------------|-----------|
| **延迟** | 分钟~小时 | 秒~分钟 | 亚秒~秒 | 毫秒~秒 |
| **吞吐** | 极高 | 极高 | 中高 | 高 |
| **适用** | 历史遗留 | 离线 ETL 主力 | Ad-hoc 交互 | 实时/CDC |
| **现状** | 计算被替代 | 离线主力 | 交互式主力 | 实时主力 |

### ③ Hive Metastore 为何是数据湖生态的基石

```
传统数仓：HMS 只管 Hive 表
    ↓ 演进
数据湖：Iceberg/Paimon/Hudi 仍复用 HMS 作为 Catalog
    ↓ 原因
    • 所有引擎已有 HMS 集成，无需重造轮子
    • 表/分区/血缘/权限统一管理
    • 渐进迁移：先换存储格式，再换表格式
```

> 📌 **加分点**：Iceberg/Paimon 要替代的是 Hive **表格式**（不是 Metastore）。痛点：① 无真正 ACID；② 分区与文件系统强耦合；③ Schema Evolution 弱；④ 无 Snapshot/Time Travel。湖表解决了这些，但仍复用 HMS 作 Catalog。（接 D37）

> **记忆口诀**：**Hive 算已退位，Meta 仍是基；湖表新格式，Catalog 还用它**。

---

## Step 4 陷阱：分区过多、动态分区、统计信息缺失

### 四大高频陷阱

| # | 陷阱 | 现象 | 对策 |
|---|------|------|------|
| ① | **分区过多** | NameNode 内存飙升 | 单表分区 < 万级；按天非按小时 |
| ② | **动态分区爆炸** | Reducer×分区 = 海量小文件 | `DISTRIBUTE BY dt`；限 `max.dynamic.partitions` |
| ③ | **分区裁剪失效** | 全表扫描 | WHERE 直写分区列，不套函数 |
| ④ | **统计信息缺失** | CBO 选错 Join 策略 | ETL 后跑 `ANALYZE TABLE` |

### 分区裁剪失效速查

| 写法 | 裁剪 | 修正 |
|------|------|------|
| `WHERE dt = '2024-06-01'` | ✅ | — |
| `WHERE dt BETWEEN '...' AND '...'` | ✅ | — |
| `WHERE substr(dt,1,7) = '2024-06'` | ❌ | `dt >= '2024-06-01' AND dt < '2024-07-01'` |
| `WHERE dt = 20240601`（STRING 列，无引号触发隐式转换） | ❌ | `WHERE dt = '2024-06-01'` |

### 动态分区小文件治理

```sql
-- 问题：100 Reducer × 30 分区 = 3000 小文件（每个 Reducer 向每个分区各写一个文件）
-- 解决：DISTRIBUTE BY 让同一分区的数据路由到同一 Reducer
INSERT OVERWRITE TABLE target PARTITION(dt)
SELECT col1, col2, dt FROM source
DISTRIBUTE BY dt SORT BY dt, col1;
-- 效果：每个分区约 1 个文件 → 共约 30 个文件（而非 900）
```

> **记忆口诀**：**分区莫套函，类型要对齐；动分加 DISTRIBUTE，统计别忘记**。

---

## Step 5 面试话术

### 问：Hive 分区和分桶的区别与适用场景？

> 分区和分桶是两种不同粒度的数据组织方式。**分区**按列值将数据放入不同子目录，物理上是**目录级**隔离，核心价值是**分区裁剪**——WHERE 条件只扫描相关分区，适合低基数字段如日期。**分桶**对某列做 hash 分散到固定数量文件，物理上是**文件级**组织，核心价值是**优化 Join**——相同列和桶数的两表可做 Bucket Map Join，适合高基数字段如 user_id。生产中通常**组合使用**：按日期分区保证查询效率，分区内按主键分桶优化 Join。分区不宜超万级，否则 NameNode 压力大；桶数取 2 的幂次方便扩展。

### 追问：Hive Metastore 的作用是什么？

> HMS 是大数据生态的**元数据中心**，用 RDBMS 存储所有表的 schema、分区、位置和属性。Spark/Flink/Presto/Trino 都通过 Thrift RPC 访问同一个 HMS。这意味着：① Spark 建的表 Flink 也能查；② 改表结构一处生效；③ 血缘和权限基于统一元数据实现。所以 Hive 现代核心价值不是"计算"，而是"元数据管理"和"存储规范"。

### 追问：为什么 Iceberg/Paimon 要替代 Hive 表格式？

> Hive 表格式痛点：① 无真正 ACID；② 分区与文件系统强耦合，rename 成本高；③ Schema Evolution 弱；④ 无 Snapshot/Time Travel。湖表引入 manifest/snapshot 层解决这些问题，同时仍可复用 HMS 作 Catalog，实现渐进升级。

### 追问：ORC 和 Parquet 怎么选？

> 性能差异不超过 10%，选型看**生态兼容性**。多引擎混用推荐 Parquet（跨引擎标准 + 湖表默认）。纯 Hive 老系统可用 ORC（索引更强）。我们项目用 Parquet+Snappy，因为下游有 Presto 即席查询和 Flink 实时入仓。

---

## Step 6 在线教育业务案例（数仓三角）

> 三个场景对应 Hive 数仓最高频的业务：  
> **ODS 外部表防误删** / **分区裁剪提速** / **动态分区小文件治理**

### 案例一：学员心跳 ODS 表 — 外部表防误删

#### 业务背景

教务系统每天产出 2 亿条心跳日志，Flink 消费 Kafka 写入 HDFS。ODS 作为原始镜像，误 DROP 恢复成本极高。

#### 三要素

| 维度 | 分析 |
|------|------|
| 根因 | ODS 由上游产出，不应被数仓操作误删 |
| 信号 | 曾发生 DROP TABLE 导致 3 天原始数据丢失 |
| 对策 | ODS 一律 **EXTERNAL TABLE**；DROP 只删元数据，文件保留可重新注册 |

```
App 心跳 → Kafka → Flink → HDFS
                                ↓
                   CREATE EXTERNAL TABLE ... LOCATION '...'
```

### 案例二：月度学情报表 — 分区裁剪提速 100x

#### 业务背景

家长端「月度学情报告」查 DWD 明细表，5 亿条/月。全表扫描 40 分钟。

#### 三要素

| 维度 | 分析 |
|------|------|
| 根因 | 用 `date_format(event_time,'yyyy-MM')` 过滤，未直写分区列 `dt`，裁剪失效 |
| 信号 | EXPLAIN 显示扫描 365 个分区而非目标 30 个 |
| 对策 | 改写 `WHERE dt >= '2024-06-01' AND dt < '2024-07-01'` |

```
优化前：365 分区 → 40 min
优化后：30 分区  → 25 sec（~100x）
```

### 案例三：ETL 回刷 — 动态分区小文件治理

#### 业务背景

算法团队回刷 90 天 DWD 明细用于模型训练，动态分区产生 18000 小文件，NameNode 告警。

#### 三要素

| 维度 | 分析 |
|------|------|
| 根因 | 200 Reducer × 90 分区 = 18000 小文件 |
| 信号 | NameNode 堆内存 40% → 85% |
| 对策 | `DISTRIBUTE BY dt` + 小文件合并任务 |

```
优化前：200×90 = 18000 文件
优化后：DISTRIBUTE BY → ~90 文件（每分区约 1 个）+ 合并任务进一步收敛
```

### 三案例对照总表

| 案例 | 主题 | 症状 | 对策 |
|------|------|------|------|
| ODS 心跳表 | 外部表防误删 | DROP 丢数据 | EXTERNAL TABLE |
| 月度学情报表 | 分区裁剪 | 全表扫描 40min | WHERE 直写分区列 |
| ETL 回刷 | 动态分区小文件 | NameNode 告警 | DISTRIBUTE BY + 合并 |

---

## 验收清单

| # | 验收项 | 验证方式 |
|---|--------|----------|
| ① | 能讲 Metastore 作用及多引擎共享机制 | Step 1① 架构图 |
| ② | 能区分内部表 vs 外部表并说出选用原则 | Step 1② 决策树 |
| ③ | 能讲清分区 vs 分桶的区别与适用场景 | Step 1③ 对比表 + 面试话术 |
| ④ | 能独立完成建分区表→写入→查询→看目录 | Step 2 实操全流程 |
| ⑤ | 能识别分区裁剪失效写法并修正 | Step 4 裁剪失效速查表 |
| ⑥ | 能讲 4 类陷阱及对策 | Step 4 陷阱表 + 记忆口诀 |
| ⑦ | 能脱稿讲"Hive 为何是数据湖基石" | Step 3③ + 加分点 |
| ⑧ | 能讲三个业务案例的三要素 | Step 6 三案例 |

---

## 运行速查

```bash
# 1. 启动 Spark + Hive（本地最轻量）
pyspark --master local[4] --conf spark.sql.warehouse.dir=/tmp/spark-warehouse

# 2. Docker 启动 Metastore（仅元数据服务，端口 9083）
docker run -d --name hive-metastore -p 9083:9083 \
  -e SERVICE_NAME=metastore apache/hive:4.0.0

# 2b. Docker 启动 HiveServer2（含内嵌 Metastore，可 beeline 连接）
docker run -d --name hiveserver2 -p 10000:10000 -p 10002:10002 \
  -e SERVICE_NAME=hiveserver2 apache/hive:4.0.0

# 3. beeline 连接（需先启动 hiveserver2 容器）
beeline -u jdbc:hive2://localhost:10000/default

# 4. 查看数据目录（本地 Spark 内嵌 Hive 用下方路径；集群 HDFS 用 LOCATION 路径）
ls /tmp/spark-warehouse/edu_demo.db/dwd_learning_detail/
# hdfs dfs -ls /data/warehouse/ods/ods_student_heartbeat/

# 5. 验证分区裁剪
EXPLAIN SELECT * FROM dwd_learning_detail WHERE dt = '2024-06-01';

# 6. 收集统计信息（Hive 语法；Spark 用 FOR ALL COLUMNS）
ANALYZE TABLE dwd_learning_detail COMPUTE STATISTICS FOR COLUMNS;
```

---

## 与其他章节的关系

| 后续/关联章节 | 本章铺垫 |
|--------------|---------|
| D22 Spark 概览 | Spark enableHiveSupport 连接 Metastore 的实操基础 |
| D24 Spark SQL 实操 | 本章分区表操作 → Spark SQL ETL 完整 Demo |
| D25 Spark 调优 | 分区裁剪、小文件、统计信息 → Spark 读取优化 |
| D27 流批一体叙事 | Hive 表 → 湖表演进的动机与路径 |
| D37 湖表专题 | 本章加分点 → 湖表格式如何解决 Hive 表痛点 |
| D41 流批一体落地 | Metastore 统一 Catalog → Flink 写 + Spark 读同一张湖表 |
| 阶段一 Flink Checkpoint | Flink 写入 Hive 分区表的 Exactly-once 保障 |
