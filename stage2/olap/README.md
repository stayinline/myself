# Stage 2 · OLAP 引擎与查询优化

本目录按 **选型 → 优化通法 → 引擎深入 → 写入链路** 组织，每个主题含 **原理** + **实战** 两份文档。

## 推荐阅读顺序

| 顺序 | 模块 | 原理 | 实战 | 核心问题 |
|------|------|------|------|---------|
| ① | 引擎选型 | [原理](引擎选型/原理.md) | [实战](引擎选型/实战.md) | CH / Doris / StarRocks 怎么选？ |
| ② | 查询优化通法 | [原理](查询优化通法/原理.md) | [实战](查询优化通法/实战.md) | 慢查询怎么系统性优化？ |
| ③ | StarRocks 入门 | [原理](starRocks入门/原理.md) | [实战](starRocks入门/实战.md) | FE/BE、四种表模型、Bucket 设计 |
| ④ | ClickHouse 物化视图 | [原理](clickhouse物化视图/原理.md) | [实战](clickhouse物化视图/实战.md) | MV 触发机制、AggregatingMergeTree |
| ⑤ | 实时 OLAP 写入链路 | [原理](实时OLAP写入链路/原理.md) | [实战](实时OLAP写入链路/实战.md) | Flink 攒批、Exactly-Once |
| ⑥ | StarRocks 实操 | [原理](starRocks实操/原理.md) | [实战](starRocks实操/实战.md) | 异步 MV、Routine Load、PK 实操 |

## 模块关系

```
引擎选型（定方向）
    ↓
查询优化通法（通用方法论）
    ↓
┌───────────────┬────────────────┐
│ StarRocks 入门 │ ClickHouse MV   │  ← 分引擎深入
└───────┬───────┴────────┬───────┘
        ↓                ↓
   StarRocks 实操    实时写入链路（Flink → CH/SR）
```

## 关键结论速记

- **ClickHouse**：单表扫描 / 日志 / 单表 MV 极致；JOIN 与高并发是短板
- **StarRocks / Doris**：MPP + CBO + 实时 Upsert；多表 JOIN 与交互式分析更强
- **优化五板斧**：分区裁剪 → 排序键 → Skip Index → 预聚合 MV → 采样
- **写入铁律**：必须攒批；CH 注意 Part 爆炸；SR 用 Flink Connector 保 Exactly-Once

## 关联章节

- 前置：[stage2/hive](../hive/README.md)（Metastore、分区裁剪）
- 后续：[stage2/数据湖](../数据湖/README.md)（Iceberg/Paimon 写入 OLAP、流批一体落地）
