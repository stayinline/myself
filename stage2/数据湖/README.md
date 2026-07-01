# Stage 2 · 数据湖（Lakehouse）

本目录按 **概览 → 引擎深入 → 选型 → 落地架构** 组织，每个主题含 **原理** + **实战** 两份文档。

## 推荐阅读顺序

| 顺序 | 模块 | 原理 | 实战 | 核心问题 |
|------|------|------|------|---------|
| ① | 数据湖概览 | [原理](数据湖概览/原理.md) | [实战](数据湖概览/实战.md) | 为什么要上数据湖？Lakehouse 解决什么？ |
| ② | Apache Iceberg | [原理](Iceberg/原理.md) | [实战](Iceberg/实战.md) | 快照、隐藏分区、ACID 怎么实现？ |
| ③ | Paimon 存储结构与表类型 | [原理](Paimon/存储结构与表类型/原理.md) | [实战](Paimon/存储结构与表类型/实战.md) | LSM、PK/Append 表、Merge Engine |
| ④ | Paimon 流读与 Changelog | [原理](Paimon/流读与Changelog与Compaction/原理.md) | [实战](Paimon/流读与Changelog与Compaction/实战.md) | changelog-producer 怎么选？Compaction 怎么治小文件？ |
| ⑤ | 三大湖对比与选型 | [原理](三大湖对比与选型/原理.md) | [实战](三大湖对比与选型/实战.md) | Iceberg / Hudi / Paimon 怎么选？ |
| ⑥ | 流批一体落地架构 | [原理](流批一体落地架构/原理.md) | [实战](流批一体落地架构/实战.md) | 如何用 Paimon 串联端到端方案？ |

## 模块关系

```
数据湖概览（Why + Lakehouse 定义）
    ↓
Iceberg（通用 Table Format 基线）
    ↓
Paimon 存储结构 → Paimon 流读/Changelog（流原生能力）
    ↓
三大湖对比选型（决策框架）
    ↓
流批一体落地架构（端到端串联）
```

## 关键结论速记

- **Lakehouse 核心**：Table Format 在开放存储上提供 ACID、快照、Upsert、Schema Evolution
- **Changelog 四类型**（Flink RowKind）：`+I` 插入、`-D` 删除、`-U` 更新前、`+U` 更新后
- **读多选 Iceberg，写多选 Paimon**；CDC + 完整 Changelog + 高频 Upsert → Paimon PK Table
- **流批一体 Level 2**：同一张湖表同时支持 Streaming Read 与 Batch Read
- **Compaction**：MOR/LSM 的固有代价；生产环境推荐 `write-only` + 独立 Compaction Job

## 关联章节

- 前置：[stage2/hive](../hive/README.md)（Metastore、Hive 表痛点）
- 前置：[stage2/hive/流批一体叙事](../hive/流批一体叙事/原理.md)（Lambda → Lakehouse 演进）
- 后续：[stage2/olap](../olap/README.md)（Paimon Streaming Read → Doris/StarRocks）
- 打卡计划：[打卡明细-阶段二](../打卡明细-阶段二-广度与加分(第4-6周).md) D36–D42
