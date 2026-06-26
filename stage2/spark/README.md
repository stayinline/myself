# Stage 2 · Spark 计算引擎

本目录按 **概览定位 → 执行原理 → SQL 实操 → 调优入门** 由浅入深组织，每个模块含 **学习指南**（由浅入深的主线）+ **原理** + **实战** 三份文档。

## 推荐阅读顺序

| 顺序 | 模块 | 学习指南 | 原理 | 实战 | 核心问题 |
|------|------|----------|------|------|---------|
| D22 | 概览与定位 | [指南](概览与定位/Spark概览与定位-学习指南.md) | [原理](概览与定位/原理.md) | [实战](概览与定位/实战.md) | Spark 是什么？vs Flink 怎么选？ |
| D23 | 执行原理 | [指南](执行原理/Spark执行原理-学习指南.md) | [原理](执行原理/原理.md) | [实战](执行原理/实战.md) | Job→Stage→Task、Shuffle、容错 |
| D24 | SQL 实操 | [指南](SQL实操/SparkSQL实操-学习指南.md) | [原理](SQL实操/原理.md) | [实战](SQL实操/实战.md) | DataFrame/SQL、Catalyst、端到端 ETL |
| D25 | 调优入门 | [指南](调优入门/Spark调优入门-学习指南.md) | [原理](调优入门/原理.md) | [实战](调优入门/实战.md) | 数据倾斜、内存模型、AQE |

## 模块关系

```
概览与定位（架构三角 + RDD/DF/Dataset 抽象 + vs Flink 选型）
    ↓
执行原理（宽窄依赖切 Stage、Shuffle 是瓶颈、Lineage/Checkpoint 容错）
    ↓
SQL 实操（Catalyst 四阶段、谓词下推/列裁剪、批 ETL、小文件/分区陷阱）
    ↓
调优入门（倾斜三板斧：Salting / Broadcast / AQE，内存与分区调优）
```

## 关键结论速记

- **定位**：Spark = batch-first 计算引擎，不做存储也不做资源分配；流处理（Structured Streaming）本质是微批，延迟下限秒级
- **抽象演进**：RDD → DataFrame → Dataset，越高层 Catalyst 优化空间越大；DataFrame API 与 Spark SQL 性能完全等价
- **执行模型**：一次 Action = 一个 Job；遇宽依赖（Shuffle）切 Stage；Stage 数 = 宽依赖数 + 1；Task 数 = 分区数
- **Shuffle 是万恶之源**：磁盘 IO + 网络 + 序列化三重开销，调优 80% 围绕它；`reduceByKey` 优于 `groupByKey`（map-side combine）
- **调优三板斧**：大小表 Join → Broadcast；GroupBy 倾斜 → Salting（对应 Flink 两阶段聚合）；Spark 3.x → AQE 运行时自适应兜底
- **ETL 三陷阱**：小文件靠 AQE 合并、Schema 要显式指定、动态分区写入必设 `partitionOverwriteMode=dynamic`

## 关联章节

- 前置：[stage2/hive](../hive/README.md)（Hive Metastore、分区裁剪，Spark on Hive 复用元数据）
- 平行：阶段一 Flink（Checkpoint、State、两阶段聚合，多处与 Spark 对照）
- 后续：[stage2/olap](../olap/README.md)、流批一体叙事（Paimon/Iceberg 让 Spark 批读 + Flink 流写同一张表）
