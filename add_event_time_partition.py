#!/usr/bin/env python3
"""
S3 Tables Iceberg: 给 mars_cl_log 添加 event_time 作为 identity 分区字段

使用：
  # 1. 预览（不写入，强烈建议第一次先跑）
  DRY_RUN=1 python add_event_time_partition.py

  # 2. 实际执行
  python add_event_time_partition.py

  # 3. 回滚（如果需要撤销）
  ROLLBACK=1 python add_event_time_partition.py

依赖：
  python3 -m venv /tmp/pyice
  /tmp/pyice/bin/pip install "pyiceberg[glue,s3fs]" pyarrow
  /tmp/pyice/bin/python add_event_time_partition.py
"""

import logging
import os
import sys
import time

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import CommitFailedException, NoSuchTableError
from pyiceberg.transforms import IdentityTransform

# =========================
# 配置 - 改成你的实际值
# =========================
AWS_ACCOUNT_ID = "305996241648"
AWS_REGION = "us-east-1"
BUCKET_NAME = "mars-log-iceberg"       # S3 Tables bucket 名（不是 ARN）
NAMESPACE = "mars_log"
TABLE_NAME = "mars_cl_log"

SOURCE_COLUMN = "event_time"           # 现有的 varchar 列，格式如 '2026-06-05-08'
PARTITION_NAME = "event_time_part"     # 分区字段名，不能和源列同名

# =========================
# 运行模式
# =========================
DRY_RUN = os.environ.get("DRY_RUN") == "1"
ROLLBACK = os.environ.get("ROLLBACK") == "1"
MAX_RETRY = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("add-event-time-partition")


def get_catalog():
    """通过 S3 Tables Iceberg REST endpoint 连接 catalog（SigV4 认证）。"""
    return load_catalog(
        "s3tables",
        type="rest",
        uri=f"https://s3tables.{AWS_REGION}.amazonaws.com/iceberg",
        warehouse=f"arn:aws:s3tables:{AWS_REGION}:{AWS_ACCOUNT_ID}:bucket/{BUCKET_NAME}",
        **{
            "rest.sigv4-enabled": "true",
            "rest.signing-name": "s3tables",
            "rest.signing-region": AWS_REGION,
        },
    )


def inspect(table, label):
    """打印表的当前 partition spec 和元信息。"""
    log.info("=" * 60)
    log.info("[%s] Spec: %s", label, table.spec())
    log.info("[%s] All spec ids: %s", label, list(table.specs().keys()))
    snap = table.current_snapshot()
    log.info(
        "[%s] Current snapshot id: %s",
        label,
        snap.snapshot_id if snap else "None",
    )
    log.info("=" * 60)


def has_partition_field(table, name):
    """检查 partition spec 里是否已经有这个字段（幂等性检查）。"""
    return any(f.name == name for f in table.spec().fields)


def with_retry(fn):
    """commit 操作的重试装饰器 - 处理 Iceberg 乐观锁冲突。

    Firehose 在高频写入时可能正好和你的 commit 撞上版本，
    Iceberg 会抛 CommitFailedException - 这不是错误，重试即可。
    """
    last_err = None
    for attempt in range(1, MAX_RETRY + 1):
        try:
            return fn()
        except CommitFailedException as e:
            last_err = e
            wait = 2 ** (attempt - 1)
            log.warning(
                "Commit conflict (attempt %d/%d) — retrying in %ds: %s",
                attempt,
                MAX_RETRY,
                wait,
                e,
            )
            time.sleep(wait)
    log.error("All %d retries failed: %s", MAX_RETRY, last_err)
    sys.exit(2)


def add_partition(catalog):
    """添加 identity 分区字段到表。"""
    table = catalog.load_table(f"{NAMESPACE}.{TABLE_NAME}")
    inspect(table, "BEFORE")

    # 安全检查 1: 源列必须存在
    cols = [f.name for f in table.schema().fields]
    if SOURCE_COLUMN not in cols:
        log.error(
            "Source column '%s' not in schema. Available: %s",
            SOURCE_COLUMN,
            cols,
        )
        sys.exit(1)

    # 安全检查 2: 分区字段名不能和现有列冲突
    if PARTITION_NAME in cols:
        log.error(
            "Partition field name '%s' collides with existing column. "
            "Choose a different PARTITION_NAME.",
            PARTITION_NAME,
        )
        sys.exit(1)

    # 安全检查 3: 幂等 - 已存在则跳过
    if has_partition_field(table, PARTITION_NAME):
        log.warning("Partition '%s' already exists. Nothing to do.", PARTITION_NAME)
        return

    if DRY_RUN:
        log.warning(
            "DRY_RUN=1 → Would add: identity(%s) AS %s",
            SOURCE_COLUMN,
            PARTITION_NAME,
        )
        return

    log.info("Adding partition: identity(%s) AS %s", SOURCE_COLUMN, PARTITION_NAME)

    def _commit():
        # 每次重试都重新 load_table，拿最新版本
        t = catalog.load_table(f"{NAMESPACE}.{TABLE_NAME}")
        with t.update_spec() as update:
            update.add_field(
                source_column_name=SOURCE_COLUMN,
                transform=IdentityTransform(),
                partition_field_name=PARTITION_NAME,
            )

    with_retry(_commit)

    # 重新加载确认
    table = catalog.load_table(f"{NAMESPACE}.{TABLE_NAME}")
    inspect(table, "AFTER")
    assert has_partition_field(table, PARTITION_NAME), (
        "Partition not applied — check Lake Formation permissions"
    )
    log.info(
        "✅ Partition '%s' (identity on %s) added successfully",
        PARTITION_NAME,
        SOURCE_COLUMN,
    )
    log.info(
        "→ Firehose 不需要任何改动，下一次 buffer flush 后新数据将自动写入分区目录。"
    )


def remove_partition(catalog):
    """回滚: 移除 partition 字段。"""
    table = catalog.load_table(f"{NAMESPACE}.{TABLE_NAME}")
    inspect(table, "BEFORE-ROLLBACK")

    if not has_partition_field(table, PARTITION_NAME):
        log.warning("Partition '%s' not found. Nothing to rollback.", PARTITION_NAME)
        return

    if DRY_RUN:
        log.warning("DRY_RUN=1 → Would REMOVE: %s", PARTITION_NAME)
        return

    log.info("Removing partition: %s", PARTITION_NAME)

    def _commit():
        t = catalog.load_table(f"{NAMESPACE}.{TABLE_NAME}")
        with t.update_spec() as update:
            update.remove_field(PARTITION_NAME)

    with_retry(_commit)

    table = catalog.load_table(f"{NAMESPACE}.{TABLE_NAME}")
    inspect(table, "AFTER-ROLLBACK")
    assert not has_partition_field(table, PARTITION_NAME), "Rollback did not apply"
    log.info("✅ Partition '%s' removed (rolled back)", PARTITION_NAME)


def main():
    log.info("Mode: DRY_RUN=%s ROLLBACK=%s", DRY_RUN, ROLLBACK)
    log.info(
        "Target: %s.%s on bucket %s | source=%s partition_name=%s",
        NAMESPACE,
        TABLE_NAME,
        BUCKET_NAME,
        SOURCE_COLUMN,
        PARTITION_NAME,
    )

    try:
        catalog = get_catalog()
    except Exception as e:
        log.error("Catalog connection failed: %s", e)
        log.error("Check: AWS credentials, region, warehouse ARN")
        sys.exit(1)

    try:
        if ROLLBACK:
            remove_partition(catalog)
        else:
            add_partition(catalog)
    except NoSuchTableError:
        log.error(
            "Table %s.%s not found in bucket %s",
            NAMESPACE,
            TABLE_NAME,
            BUCKET_NAME,
        )
        sys.exit(1)
    except Exception as e:
        log.exception("Unexpected error: %s", e)
        sys.exit(3)


if __name__ == "__main__":
    main()
