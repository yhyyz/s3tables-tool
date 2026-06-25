# S3 Tables 分区添加工具

给 `mars_log.mars_cl_log` 添加 `event_time` 作为 identity 分区字段，
**Firehose 配置无需任何改动**。

## 文件说明

| 文件 | 用途 |
|---|---|
| `setup.sh` | 安装 PyIceberg + pyarrow 到独立 venv |
| `add_event_time_partition.py` | 主脚本：添加/回滚 partition |
| `verify_partition.py` | 验证脚本：检查 partition spec 状态 |
| `README.md` | 本文档 |

## 使用流程

### 第一次（一次性）：安装依赖

```bash
bash setup.sh
```

会在 `/tmp/pyice` 创建 venv 并安装 pyiceberg + pyarrow。
如果要换路径：`VENV_DIR=/path/to/venv bash setup.sh`

### Step 1: Dry-run 预览（不修改）

```bash
DRY_RUN=1 /tmp/pyice/bin/python add_event_time_partition.py
```

输出会显示：
- 当前 partition spec（应为空 `[]`）
- 计划添加的字段：`identity(event_time) AS event_time_part`
- 不实际写入

### Step 2: 实际执行

```bash
/tmp/pyice/bin/python add_event_time_partition.py
```

成功会看到：
```
[BEFORE] Spec: []
Adding partition: identity(event_time) AS event_time_part
[AFTER] Spec: [1000: event_time_part: identity(35)]
✅ Partition 'event_time_part' (identity on event_time) added successfully
→ Firehose 不需要任何改动，下一次 buffer flush 后新数据将自动写入分区目录。
```

执行时间约 3-5 秒。期间 Firehose 写入不会中断。

### Step 3: 验证

```bash
/tmp/pyice/bin/python verify_partition.py
```

也可以在 Athena 上查文件路径：

```sql
SELECT file_path 
FROM "s3tablescatalog/lakehouse-agent-poc".mars_log."mars_cl_log$files"
WHERE regexp_like(file_path, '/event_time_part=')
ORDER BY file_path DESC LIMIT 10;
```

期望（Firehose buffer flush 后）：
```
.../data/event_time_part=2026-06-25-06/00001-...parquet
.../data/event_time_part=2026-06-25-07/00002-...parquet
```

### Step 4 (可选): 回滚

如果发现问题需要撤销分区：

```bash
ROLLBACK=1 /tmp/pyice/bin/python add_event_time_partition.py
```

注意：回滚只是移除 partition spec，**已经写入分区目录的旧文件不会移动**。
Iceberg 通过 spec_id 自动处理多版本，查询照常工作。

## 关键配置（在脚本顶部）

```python
AWS_ACCOUNT_ID = "305996241648"
AWS_REGION = "us-east-1"
BUCKET_NAME = "lakehouse-agent-poc"  # 改成你的真实 S3 Tables bucket 名
NAMESPACE = "mars_log"
TABLE_NAME = "mars_cl_log"
SOURCE_COLUMN = "event_time"
PARTITION_NAME = "event_time_part"  # 注意不能和源列同名
```

## 前置 IAM 权限

跑脚本的身份（IAM user / role）需要：

```json
{
  "Effect": "Allow",
  "Action": [
    "s3tables:GetTable",
    "s3tables:UpdateTableMetadataLocation",
    "s3tables:GetTableMetadataLocation",
    "s3tables:GetNamespace",
    "s3tables:GetTableBucket",
    "lakeformation:GetDataAccess"
  ],
  "Resource": "*"
}
```

加上 Lake Formation 权限：

```bash
aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=<your-iam-arn> \
  --resource '{"Table":{
    "CatalogId":"305996241648:s3tablescatalog/lakehouse-agent-poc",
    "DatabaseName":"mars_log",
    "Name":"mars_cl_log"
  }}' \
  --permissions ALTER DESCRIBE \
  --region us-east-1
```

## 分区粒度提示

`event_time` 是 `YYYY-MM-DD-HH` 小时级，所以是**小时分区**：

| 时间窗口 | 分区数 |
|---|---|
| 1 天 | 24 |
| 30 天 | 720 |
| 1 年 | 8760 |

Iceberg 推荐分区数 < 10K，720 在安全区。
如果只做日级查询，建议改用 `truncate(10, "timestamp")` 做日分区，
分区数减少 24 倍，metadata 更轻。

## 查询时如何触发分区裁剪

加完分区后查询要用 `event_time` 字段过滤：

```sql
-- ✅ 触发分区裁剪（精确小时）
WHERE event_time = '2026-06-15-08'

-- ✅ 触发分区裁剪（小时范围）  
WHERE event_time >= '2026-06-15-08' AND event_time < '2026-06-15-12'

-- ✅ 触发分区裁剪（整天）
WHERE event_time LIKE '2026-06-15-%'
WHERE event_time >= '2026-06-15' AND event_time < '2026-06-16'

-- ❌ 不会触发分区裁剪（用 timestamp 字段）
WHERE "timestamp" >= '2026-06-15'
```

## 常见错误处理

### `Cannot create identity partition sourced from different field`
分区字段名 (`PARTITION_NAME`) 不能和源列 (`SOURCE_COLUMN`) 同名。
脚本里已经预防了 - 用了 `event_time_part` 而不是 `event_time`。

### `Lakeformation.AccessDenied`  
Lake Formation 权限不够。运行 README 里的 `grant-permissions` 命令。

### `CommitFailedException` (脚本会自动重试)
表在你 commit 之前被 Firehose 修改了（高频写入场景）。
脚本默认重试 5 次，指数退避。

### `NoSuchTableError`
检查 `BUCKET_NAME` / `NAMESPACE` / `TABLE_NAME` 是否正确。
也可能是 Lake Formation 上 DESCRIBE 权限缺失。

## Firehose 行为预期

执行成功后：

1. **当前 buffer 中的记录**: 可能按旧 spec (无分区) 写入 - Firehose schema 缓存滞后几秒到几分钟
2. **下一次 buffer flush 起**: 新文件路径自动变成 `.../data/event_time_part=YYYY-MM-DD-HH/...`
3. **旧数据文件**: 保持原位 (`.../data/00001-...`), 不会迁移
4. **查询时**: Athena/Spark 自动识别多个 spec, 结果正确

## 实测验证

这套方案在我们的测试环境（`firehose-test-305996241648`）已完整验证过：
- 90 条记录在加分区前写入 → 在 `/data/` 无前缀
- 加分区后继续写 120 条 → 自动分到 3 个 `event_day=YYYY-MM-DD/` 目录
- Firehose 配置完全没改

详细测试日志参见对话记录。
