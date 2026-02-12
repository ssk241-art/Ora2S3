"""
========================================================================================================================
METADATA-DRIVEN ORA TO S3 DATA EXPORT FRAMEWORK
========================================================================================================================

Script_Author  : Capgemini

Overview
--------
Mmetadata-driven Python framework to move data reliably between Oracle and Amazon S3.
It is designed for enterprise ETL operations (e.g., Informatica PowerCenter, Control-M, shell schedulers),
with strong observability, deterministic behavior, and recovery/resume support.
----------

Called with runtime parameters:
  --workflow_nm, --mapping_nm, --job_nm, --run_id
  --ora_dsn, --ora_user, --ora_password
  --cfg_table, --audit_table

Configuration Source
--------------------
Reads CONFIG_JSON (CLOB) from an Oracle control-table row identified by:
  (workflow_nm, mapping_nm, job_nm) and is_active='Y'
---------------------------

------------------------------------------------------------------------------------------------------------------------
FEATURES
------------------------------------------------------------------------------------------------------------------------
1) Modes
   - TABLE_TO_S3 : Extract Oracle rows into local files (windowed) and upload to S3
   - S3_TO_S3    : Download matching source S3 objects (windowed by filename datetime), pipeline, upload to target

2) Load Modes / Windowing
   - Manual     (load.type="M"): uses config.load.start and config.load.end
   - Automated  (load.type="A"): uses control-table watermark columns start_time/end_time
       * If watermark is NULL => bootstrap from previous interval or today-only (config)
       * Optional cap_mode / lag_days / lag_intervals to prevent future windows
       * Optional max_windows_per_run to throttle

3) Granularity / Interval Windows
   - load.granularity: "date" | "hour" | "minute"
   - load.interval_minutes: 1440 (date), 60 (hour), 10, 15, etc.

4) Oracle Extraction (streaming, scalable)
   - fetchmany streaming
   - config-driven select list OR SELECT *
   - config-driven window filter on source.window_column / watermark_column
   - config-driven order_by and extra_where filter
   - Always produces a file; if no rows => file contains header only (if header=true)

5) File Format (config-driven)
   - delimiter, header, null_value, quoting(all|minimal|none), quotechar, escapechar, encoding, line_ending
   - line_ending supports "\\n" and "\\r\\n" in JSON and is normalized

6) File Splitting (optional)
   - file.split.enabled + file.split.size (e.g., "1 GB")
   - line-safe splitting into file_1.txt, file_2.txt ...
   - option to include header per part

7) Compression (optional, config-driven)
   - compression.enabled=true
   - compression.type: gzip | bz2 | zip
   - extension derived from compression.type (not hardcoded)

8) Encryption (optional, config-driven)
   - encryption.enabled=true
   - gpg encryption -> .gpg (recipient from config)

9) Multi-destination S3 Upload (config-driven)
   - target.destinations: list of target S3 locations
   - Each destination has: name, s3_path, key_template, exists_behavior, audit_status_on_skip
   - Credentials selected per bucket using aws_profiles.json (multi access keys supported)
   - Uploads the SAME artifact to MULTIPLE destinations (e.g., bucket1 + bucket2)

10) S3 Object Existence Behavior Per Destination
   - exists_behavior: skip | overwrite | fail
   - When skip and object exists: action=SKIPPED; audit uses audit_status_on_skip (e.g., SKIPPED)

11) Done File (optional)
   - done_file.enabled=true
   - Always generated after successful uploads (all destinations) for a window
   - done_file.upload.enabled=true uploads done file to S3 (to selected destinations or all)

12) Audit Logging (bulk MERGE)
   - Bulk MERGE (executemany)
   - Audit columns supported:
       JOB_NM, MAPPING_NM, WORKFLOW_NM, START_DTTM, END_DTTM, STATUS, RUN_ID,
       REJECT_COUNT, TARGET_COUNT, TARGETNAME, STAGE, ERROR_MSG, CREATED_DTTM, UPDATED_DTTM
   - ERROR_MSG is truncated to 4000 characters (Oracle VARCHAR2 typical limit)

13) Stage-aware Checkpointing & Resume (ANY_RUN / SAME_RUN)
   - Writes stage markers after each stage completion/failure and flushes audit in bulk after each stage.
   - TABLE_TO_S3 stages (in order):
       1) Data Extract
       2) Data Split      (only if split executes)
       3) Data Compress   (only if enabled)
       4) Data Encrypt    (only if enabled)
       5) Data Upload
   - S3_TO_S3 stages (in order):
       1) Source Check (data + done file check)
       2) Data Download
       3) Data Upload
   - Resume rule (no config changes):
       * If audit shows "<Stage> Completed" AND the expected intermediate artifact exists on disk,
         the stage is skipped and the next stage proceeds.
       * If intermediate artifacts are missing, the pipeline restarts from the earliest stage.

14) Watermark Update (A-mode)
   - On overall success, updates control-table start_time/end_time to last processed window boundaries

15) Purge Policies (optional)
   - runtime.purge.enabled=true
   - purge.when: on_success | always
   - purge.rules: path + include_globs + older_than_days

16) Locking
   - Prevent concurrent run per (workflow_nm, mapping_nm, job_nm) using atomic lock file

17) Python 3.6 Compatible
   - Avoids newer syntax and APIs; uses universal_newlines in subprocess

========================================================================================================================
"""
import argparse
import bz2
import csv
import datetime as dt
import fnmatch
import glob
import gzip
import json
import os
import re
import shutil
import shlex
import subprocess
import sys
import time
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
import threading
import pandas as pd
import requests

import boto3
import pytz
from botocore.exceptions import ClientError

import cx_Oracle as oracle  # type: ignore




# ============================================================================
# Exceptions
# ============================================================================
class FrameworkError(Exception):
    """Base framework exception."""


class ConfigError(FrameworkError):
    """Raised when configuration is invalid or incomplete."""


class LockError(FrameworkError):
    """Raised when a concurrent run is detected."""


class ExtractError(FrameworkError):
    """Raised when Oracle extraction fails."""


class UploadError(FrameworkError):
    """Raised when S3 upload fails."""


class EncryptError(FrameworkError):
    """Raised when encryption fails."""


# ============================================================================
# Constants
# ============================================================================
EASTERN_TZ = pytz.timezone("America/New_York")
ERROR_MSG_MAX_LEN = 4000

# Stage labels requested (human-friendly)
STAGE_DATA_EXTRACT = "Data Extract"
STAGE_DATA_SPLIT = "Data Split"
STAGE_DATA_COMPRESS = "Data Compress"
STAGE_DATA_ENCRYPT = "Data Encrypt"
STAGE_DATA_UPLOAD = "Data Upload"

STAGE_SOURCE_CHECK = "Source Check"
STAGE_DATA_DOWNLOAD = "Data Download"

STAGE_COMPLETED = "Completed"
STAGE_FAILED = "Failed"

LOCAL_AUDIT_DEST = "" 

LOG_FILE_HANDLE = None


# ============================================================================
# Logging
# ============================================================================
def now_eastern_naive() -> dt.datetime:
    return dt.datetime.now(EASTERN_TZ).replace(tzinfo=None)


def ensure_directory(path: str) -> None:
    if path and not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)


def initialize_log_file(log_dir: str, log_filename: str) -> None:
    global LOG_FILE_HANDLE
    ensure_directory(log_dir)
    LOG_FILE_HANDLE = open(os.path.join(log_dir, log_filename), "a", encoding="utf-8")


def close_log_file() -> None:
    global LOG_FILE_HANDLE
    try:
        if LOG_FILE_HANDLE:
            LOG_FILE_HANDLE.flush()
            LOG_FILE_HANDLE.close()
    except Exception:
        pass
    LOG_FILE_HANDLE = None


def log_message(message: str) -> None:
    timestamp = now_eastern_naive().strftime("%Y-%m-%d %H:%M:%S")
    line = "{} | {}".format(timestamp, message)
    print(line)
    if LOG_FILE_HANDLE:
        LOG_FILE_HANDLE.write(line + "\n")
        LOG_FILE_HANDLE.flush()


# Convenience alias used throughout the framework
log = log_message

# ============================================================================
# Config helpers
# ============================================================================
def require_config(cfg: Dict[str, Any], key: str, path: str, allow_empty_str: bool = False) -> Any:
    if key not in cfg:
        log_message("[CONFIG][MISSING] required key not found: {}.{} (available_keys={})".format(path, key, list(cfg.keys())))
        raise ConfigError("Missing required config: {}.{}".format(path, key))
    value = cfg[key]
    if value is None:
        log_message("[CONFIG][MISSING] required key is NULL: {}.{}".format(path, key))
        raise ConfigError("Missing required config: {}.{}".format(path, key))
    if (not allow_empty_str) and isinstance(value, str) and value == "":
        log_message("[CONFIG][MISSING] required key is empty-string: {}.{}".format(path, key))
        raise ConfigError("Missing required config: {}.{}".format(path, key))
    return value


def get_config(cfg: Dict[str, Any], key: str, default: Any = None) -> Any:
    return cfg[key] if key in cfg else default


def coerce_oracle_lob_to_str(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "read"):
        return value.read()
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8")
    if isinstance(value, str):
        return value
    return str(value)


def render_template(template: str, tokens: Dict[str, str]) -> str:
    return template.format(**tokens)


def normalize_line_ending(value: str) -> str:
    if value is None:
        return "\n"
    if value == "\\n":
        return "\n"
    if value == "\\r\\n":
        return "\r\n"
    return value


def truncate_error_message(message: str) -> str:
    if not message:
        return ""
    if len(message) <= ERROR_MSG_MAX_LEN:
        return message
    return message[:ERROR_MSG_MAX_LEN]


# ============================================================================
# Retry
# ============================================================================
def run_command_capture_output(command: List[str], env: Optional[Dict[str, str]] = None) -> Tuple[int, str, str]:
    process = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, env=env)
    return process.returncode, process.stdout, process.stderr


def count_lines_wc(file_path: str) -> int:
    return_code, stdout, stderr = run_command_capture_output(["wc", "-l", file_path])
    if return_code != 0:
        raise FrameworkError("wc -l failed: {} {}".format(stdout, stderr))
    return int(stdout.strip().split()[0])



def count_lines_stream_command(cmd: str, env: Optional[Dict[str, str]] = None) -> int:
    """Run a shell pipeline that ends in `wc -l` and return the integer line count."""
    rc, out, err = run_command_capture_output(["bash", "-lc", cmd], env=env)
    if rc != 0:
        raise FrameworkError("count command failed rc={} cmd={} err={}".format(rc, cmd, (err or out or "").strip()))
    try:
        return int((out or "").strip().split()[0])
    except Exception:
        raise FrameworkError("count command returned non-int output: {}".format(out))

def compute_artifact_data_rows(file_path: str,
                               file_cfg: Dict[str, Any],
                               compression_cfg: Optional[Dict[str, Any]] = None,
                               encryption_cfg: Optional[Dict[str, Any]] = None) -> int:
    """Compute data row count for a *non-encrypted* artifact by streaming it to `wc -l`.

    Supported:
      - Plain text:   wc -l file
      - Gzip (.gz):   gzip -cd file | wc -l   (or zcat)
      - Bzip2 (.bz2): bzip2 -cd file | wc -l  (or bzcat)
      - Zip (.zip):   unzip -p file | wc -l   (expects single-file zip)

    Notes:
      - Subtracts 1 line if file.format.header=true (to return *data rows* not including header).
      - Encrypted files (.gpg/.pgp) are intentionally NOT supported here.
      - Any failure returns 0.
    """
    try:
        fmt = require_config(file_cfg, "format", "file")
        header_enabled = bool(get_config(fmt, "header", True))
    except Exception:
        header_enabled = True

    fp = file_path or ""
    lower = fp.lower()

    # IMPORTANT: Encrypted artifacts cannot be decrypted here (no private key).
    # They must never be routed to this function.
    if lower.endswith(".gpg") or lower.endswith(".pgp"):
        return 0

    try:
        if lower.endswith(".gz"):
            total_lines = count_lines_stream_command(
                "(command -v zcat >/dev/null 2>&1 && zcat {0} || gzip -cd {0}) | wc -l".format(shlex.quote(fp))
            )
        elif lower.endswith(".bz2"):
            total_lines = count_lines_stream_command(
                "(command -v bzcat >/dev/null 2>&1 && bzcat {0} || bzip2 -cd {0}) | wc -l".format(shlex.quote(fp))
            )
        elif lower.endswith(".zip"):
            total_lines = count_lines_stream_command("unzip -p {} | wc -l".format(shlex.quote(fp)))
        else:
            total_lines = count_lines_wc(fp)

        if header_enabled and total_lines > 0:
            total_lines = max(0, int(total_lines) - 1)
        return int(total_lines)
    except Exception:
        return 0



def extract_oracle_error_code(exception: Exception) -> str:
    message = str(exception) or ""
    match = re.search(r"(ORA-\d{5})", message)
    return match.group(1) if match else ""


def build_retry_policy(cfg: Dict[str, Any]) -> Dict[str, Any]:
    mode = (require_config(cfg, "mode", "config") or "").upper()
    if mode == "TABLE_TO_S3":
        table_to_s3cfg = require_config(cfg, "table_to_s3_config", "config")
        retry_policy = get_config(table_to_s3cfg, "retry_policy", None)
        runtime_cfg = require_config(table_to_s3cfg, "runtime", "table_to_s3_config")
    else:
        s3cfg = require_config(cfg, "s3_to_s3_config", "config")
        retry_policy = get_config(s3cfg, "retry_policy", None)
        runtime_cfg = require_config(s3cfg, "runtime", "s3_to_s3_config")

    if isinstance(retry_policy, dict):
        retry_policy.setdefault("max_retries", 5)
        retry_policy.setdefault("backoff_base_sec", 2)
        retry_policy.setdefault("backoff_max_sec", 60)
        return retry_policy


    return {
        "max_retries": int(get_config(runtime_cfg, "max_retries", 5) or 5),
        "backoff_base_sec": int(get_config(runtime_cfg, "backoff_base_sec", 2) or 2),
        "backoff_max_sec": int(get_config(runtime_cfg, "backoff_max_sec", 60) or 60),
    }


def retry_with_backoff(operation_fn, retry_cfg: Dict[str, Any], operation_label: str):
    max_retries = int(require_config(retry_cfg, "max_retries", "retry_policy"))
    base_seconds = int(require_config(retry_cfg, "backoff_base_sec", "retry_policy"))
    max_seconds = int(require_config(retry_cfg, "backoff_max_sec", "retry_policy"))

    last_exception = None
    for attempt in range(1, max_retries + 2):
        try:
            return operation_fn()
        except ConfigError:
            raise
        except Exception as exc:
            if extract_oracle_error_code(exc) in ("ORA-01821", "ORA-01861", "ORA-00933", "ORA-00904"):
                raise
            last_exception = exc
            if attempt >= (max_retries + 1):
                break
            sleep_seconds = min(max_seconds, base_seconds * (2 ** (attempt - 1)))
            log_message("[RETRY] {} attempt={}/{} err={} sleep={}s".format(operation_label, attempt, max_retries + 1, exc, sleep_seconds))
            time.sleep(sleep_seconds)
    raise last_exception  # type: ignore


# ============================================================================
# Locking
# ============================================================================
def sanitize_identifier(raw: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", raw or "")


def build_lock_file_path(lock_dir: str, workflow_nm: str, mapping_nm: str, job_nm: str) -> str:
    return os.path.join(lock_dir, "{}__{}__{}.lock".format(
        sanitize_identifier(workflow_nm),
        sanitize_identifier(mapping_nm),
        sanitize_identifier(job_nm),
    ))


def acquire_process_lock(lock_file_path: str) -> None:
    ensure_directory(os.path.dirname(lock_file_path))
    open_flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    try:
        fd = os.open(lock_file_path, open_flags)
        with os.fdopen(fd, "w") as file_handle:
            file_handle.write("pid={} time={}\n".format(os.getpid(), now_eastern_naive().isoformat()))
    except FileExistsError:
        raise LockError("Lock exists, active run detected: {}".format(lock_file_path))


def release_process_lock(lock_file_path: str) -> None:
    try:
        if os.path.exists(lock_file_path):
            os.remove(lock_file_path)
    except Exception as exc:
        log_message("[WARN] failed to remove lock {} err={}".format(lock_file_path, exc))


# ============================================================================
# Oracle: control + audit
# ============================================================================
def create_oracle_connection(dsn: str, user: str, password: str):
    return oracle.connect(user=user, password=password, dsn=dsn)


def fetch_config_row(conn, cfg_table: str, workflow_nm: str, mapping_nm: str, job_nm: str) -> Dict[str, Any]:
    sql = """
        SELECT workflow_nm, mapping_nm, job_nm, subject_area, is_active,
               start_time, end_time, run_id, config_json
          FROM {t}
         WHERE workflow_nm = :w
           AND mapping_nm  = :m
           AND job_nm      = :j
           AND is_active   = 'Y'
    """.format(t=cfg_table)

    cursor = conn.cursor()
    try:
        cursor.execute(sql, w=workflow_nm, m=mapping_nm, j=job_nm)
        row = cursor.fetchone()
        if not row:
            raise ConfigError("No active config found in {} for {}/{}/{}".format(cfg_table, workflow_nm, mapping_nm, job_nm))
        columns = [d[0].lower() for d in cursor.description]
        result = dict(zip(columns, row))
        result["config_json"] = coerce_oracle_lob_to_str(result.get("config_json"))
        return result
    finally:
        try:
            cursor.close()
        except Exception:
            pass


def update_control_watermark(conn,
                             cfg_table: str,
                             workflow_nm: str,
                             mapping_nm: str,
                             job_nm: str,
                             new_start: dt.datetime,
                             new_end: dt.datetime,
                             run_id: str,
                             try_update_last_upd: bool = True) -> None:
    base_sql = """
        UPDATE {t}
           SET start_time = :st,
               end_time   = :et,
               run_id     = :rid
         WHERE workflow_nm = :w
           AND mapping_nm  = :m
           AND job_nm      = :j
    """.format(t=cfg_table)

    sql_with_lastupd = """
        UPDATE {t}
           SET start_time = :st,
               end_time   = :et,
               run_id     = :rid,
               LAST_UPD_DTTM = SYSTIMESTAMP
         WHERE workflow_nm = :w
           AND mapping_nm  = :m
           AND job_nm      = :j
    """.format(t=cfg_table)

    cursor = conn.cursor()
    try:
        if try_update_last_upd:
            try:
                cursor.execute(sql_with_lastupd, st=new_start, et=new_end, rid=run_id, w=workflow_nm, m=mapping_nm, j=job_nm)
            except Exception as exc:
                if extract_oracle_error_code(exc) == "ORA-00904":
                    cursor.execute(base_sql, st=new_start, et=new_end, rid=run_id, w=workflow_nm, m=mapping_nm, j=job_nm)
                else:
                    raise
        else:
            cursor.execute(base_sql, st=new_start, et=new_end, rid=run_id, w=workflow_nm, m=mapping_nm, j=job_nm)
        conn.commit()
        log_message("[WATERMARK] updated start_time={} end_time={}".format(new_start, new_end))
    finally:
        try:
            cursor.close()
        except Exception:
            pass

def update_control_run_id_only(conn,
                               cfg_table: str,
                               workflow_nm: str,
                               mapping_nm: str,
                               job_nm: str,
                               run_id: str,
                               try_update_last_upd: bool = True) -> None:
    """
    Update ONLY run_id in the control/metadata table.
    This is intended for Manual runs (load.type == 'M') where start_time/end_time must not change.
    """

    base_sql = """
        UPDATE {t}
           SET run_id = :rid
         WHERE workflow_nm = :w
           AND mapping_nm  = :m
           AND job_nm      = :j
    """.format(t=cfg_table)

    sql_with_lastupd = """
        UPDATE {t}
           SET run_id = :rid,
               LAST_UPD_DTTM = SYSTIMESTAMP
         WHERE workflow_nm = :w
           AND mapping_nm  = :m
           AND job_nm      = :j
    """.format(t=cfg_table)

    cursor = conn.cursor()
    try:
        if try_update_last_upd:
            try:
                cursor.execute(sql_with_lastupd, rid=run_id, w=workflow_nm, m=mapping_nm, j=job_nm)
            except Exception as exc:
                if extract_oracle_error_code(exc) == "ORA-00904":
                    cursor.execute(base_sql, rid=run_id, w=workflow_nm, m=mapping_nm, j=job_nm)
                else:
                    raise
        else:
            cursor.execute(base_sql, rid=run_id, w=workflow_nm, m=mapping_nm, j=job_nm)

        conn.commit()
        log_message("[RUN_ID] updated run_id only")
    finally:
        try:
            cursor.close()
        except Exception:
            pass


def bulk_merge_audit_rows(conn, audit_table: str, rows: List[Dict[str, Any]]) -> None:
    """
    Bulk MERGE into audit table including stage + error_msg with created/updated timestamps.

    Expected audit columns:
      JOB_NM, MAPPING_NM, WORKFLOW_NM, START_DTTM, END_DTTM, STATUS, RUN_ID, REJECT_COUNT, TARGET_COUNT,
      TARGETNAME, STAGE, ERROR_MSG, CREATED_DTTM, UPDATED_DTTM
    """
    if not rows:
        return

    for row in rows:
        row["error_msg"] = truncate_error_message(str(row.get("error_msg") or ""))

    merge_sql = """
    MERGE INTO {t} tgt
    USING (
      SELECT
        :job_nm       AS job_nm,
        :mapping_nm   AS mapping_nm,
        :workflow_nm  AS workflow_nm,
        :start_dttm   AS start_dttm,
        :end_dttm     AS end_dttm,
        :status       AS status,
        :run_id       AS run_id,
        :reject_count AS reject_count,
        :target_count AS target_count,
        :targetname   AS targetname,
        :stage        AS stage,
        :error_msg    AS error_msg
      FROM dual
    ) src
    ON (
      tgt.job_nm      = src.job_nm AND
      tgt.mapping_nm  = src.mapping_nm AND
      tgt.workflow_nm = src.workflow_nm AND
      tgt.targetname  = src.targetname AND
      tgt.start_dttm  = src.start_dttm AND
      tgt.end_dttm    = src.end_dttm AND
      tgt.stage     = src.stage AND
      tgt.run_id    = src.run_id AND
      tgt.status = src.status
    )
    WHEN MATCHED THEN UPDATE SET
      tgt.reject_count = src.reject_count,
      tgt.target_count = src.target_count,
tgt.error_msg    = src.error_msg,
      tgt.updated_dttm = SYSTIMESTAMP
    WHEN NOT MATCHED THEN INSERT
      (job_nm, mapping_nm, workflow_nm, start_dttm, end_dttm, status, run_id,
       reject_count, target_count, targetname, stage, error_msg, created_dttm, updated_dttm)
    VALUES
      (src.job_nm, src.mapping_nm, src.workflow_nm, src.start_dttm, src.end_dttm, src.status, src.run_id,
       src.reject_count, src.target_count, src.targetname, src.stage, src.error_msg, SYSTIMESTAMP, SYSTIMESTAMP)
    """.format(t=audit_table)

    cursor = conn.cursor()
    try:
        cursor.executemany(merge_sql, rows)
        conn.commit()
    finally:
        try:
            cursor.close()
        except Exception:
            pass




def query_audit_stage_completed(conn,
                               audit_table: str,
                               workflow_nm: str,
                               mapping_nm: str,
                               job_nm: str,
                               start_dttm: dt.datetime,
                               end_dttm: dt.datetime,
                               targetname: str,
                               stage_label: str,
                               scope: str,
                               run_id: str,
                               success_statuses: Optional[List[str]] = None) -> bool:
    """Backward-compatible helper: return True if stage '<label> Completed' exists with success status."""
    stage_value = "{} {}".format(stage_label, STAGE_COMPLETED)
    statuses = success_statuses or ["SUCCESS", "SKIPPED"]

    # SAME_RUN honors run_id; ANY_RUN ignores it.
    if (scope or "").upper() == "SAME_RUN":
        return query_audit_stage_completed_flexible(
            conn=conn,
            audit_table=audit_table,
            workflow_nm=workflow_nm,
            mapping_nm=mapping_nm,
            job_nm=job_nm,
            run_id=run_id,
            stage_value=stage_value,
            targetname=targetname,
            window_start=start_dttm,
            window_end=end_dttm,
            scope=scope,
            success_statuses=statuses,
        )

    # ANY_RUN: allow any run_id, so we check with run_id wildcard by searching without run_id filter.
    # Keep this query simple and indexed-friendly.
    status_in = ", ".join([":s{}".format(i) for i in range(len(statuses))])
    sql = """
        SELECT 1
          FROM {t}
         WHERE workflow_nm = :w
           AND mapping_nm  = :m
           AND job_nm      = :j
           AND start_dttm  = :sd
           AND end_dttm    = :ed
           AND targetname  = :tn
           AND stage       = :stage
           AND status      IN ({status_in})
           AND ROWNUM = 1
    """.format(t=audit_table, status_in=status_in)

    binds: Dict[str, object] = dict(w=workflow_nm, m=mapping_nm, j=job_nm, sd=start_dttm, ed=end_dttm, tn=targetname, stage=stage_value)
    for i, s in enumerate(statuses):
        binds["s{}".format(i)] = s

    cur = conn.cursor()
    try:
        cur.execute(sql, binds)
        return cur.fetchone() is not None
    finally:
        try:
            cur.close()
        except Exception:
            pass



def query_audit_stage_completed_flexible(conn,
                                        audit_table: str,
                                        workflow_nm: str,
                                        mapping_nm: str,
                                        job_nm: str,
                                        stage_value: str,
                                        scope: str,
                                        run_id: str,
                                        targetname: Optional[str] = None,
                                        window_start: Optional[dt.datetime] = None,
                                        window_end: Optional[dt.datetime] = None,
                                        alternate_targetnames: Optional[List[str]] = None,
                                        alternate_stages: Optional[List[str]] = None,
                                        alternate_ranges: Optional[List[Tuple[dt.datetime, dt.datetime]]] = None,
                                        success_statuses: Optional[List[str]] = None) -> bool:
    """Return True if an audit row exists that indicates the given stage already succeeded/skipped.

    - scope = ANY_RUN: ignore RUN_ID (match any run)
    - scope = SAME_RUN: match only the provided RUN_ID
    - You can pass alternate_targetnames / alternate_stages / alternate_ranges to be backward compatible
      with earlier audit conventions (e.g., START_DTTM / END_DTTM written differently).
    """
    stage_candidates = [stage_value] + (alternate_stages or [])
    status_candidates = success_statuses or ["SUCCESS", "SKIPPED"]

    stage_binds = {f"stg{i}": v for i, v in enumerate(stage_candidates)}
    status_binds = {f"sts{i}": v for i, v in enumerate(status_candidates)}
    stage_in = ", ".join(f":stg{i}" for i in range(len(stage_candidates)))
    status_in = ", ".join(f":sts{i}" for i in range(len(status_candidates)))

    target_candidates: List[str] = []
    if targetname:
        target_candidates.append(targetname)
    if alternate_targetnames:
        target_candidates.extend([t for t in alternate_targetnames if t])

    target_clause = ""
    target_binds: Dict[str, str] = {}
    if target_candidates:
        target_binds = {f"tn{i}": v for i, v in enumerate(target_candidates)}
        target_in = ", ".join(f":tn{i}" for i in range(len(target_candidates)))
        target_clause = f" AND targetname IN ({target_in})"

    window_clause = ""
    window_binds: Dict[str, dt.datetime] = {}

    # Prefer exact match when provided
    if window_start is not None:
        window_clause = " AND start_dttm = :window_start"
        window_binds["window_start"] = window_start
    if window_end is not None:
        window_clause += " AND end_dttm = :window_end"
        window_binds["window_end"] = window_end

    # Backward-compatible range matching
    if alternate_ranges:
        range_parts = []
        for i, (rs, re_) in enumerate(alternate_ranges):
            window_binds[f"rs{i}"] = rs
            window_binds[f"re{i}"] = re_
            # Compare both start and end when possible (end might be NULL in some legacy rows)
            range_parts.append(f"(start_dttm >= :rs{i} AND start_dttm < :re{i})")
        if range_parts:
            range_expr = " OR ".join(range_parts)
            if window_clause:
                window_clause = f" AND ({window_clause.strip()[3:]} OR {range_expr})"  # remove leading 'AND'
                window_clause = " AND " + window_clause[len(" AND "):]  # normalize
            else:
                window_clause = f" AND ({range_expr})"

    sql = f"""
        SELECT 1
          FROM {audit_table}
         WHERE workflow_nm = :workflow_nm
           AND mapping_nm  = :mapping_nm
           AND job_nm      = :job_nm
           AND stage       IN ({stage_in})
           AND status      IN ({status_in})
           {target_clause}
           {window_clause}
           AND ROWNUM = 1
    """

    if (scope or "").upper() == "SAME_RUN":
        sql = sql.replace("WHERE", "WHERE run_id = :run_id AND", 1)

    binds: Dict[str, object] = {
        "workflow_nm": workflow_nm,
        "mapping_nm": mapping_nm,
        "job_nm": job_nm,
    }
    if (scope or "").upper() == "SAME_RUN":
        binds["run_id"] = run_id

    binds.update(stage_binds)
    binds.update(status_binds)
    binds.update(target_binds)
    binds.update(window_binds)


    cur = conn.cursor()
    try:
        cur.execute(sql, binds)
        return cur.fetchone() is not None
    finally:
        try:
            cur.close()
        except Exception:
            pass

def stage_to_audit_value(stage_label: str, ok: bool) -> str:
    return "{} {}".format(stage_label, STAGE_COMPLETED if ok else STAGE_FAILED)


def query_audit_has_success_status(conn,
                                  audit_table: str,
                                  workflow_nm: str,
                                  mapping_nm: str,
                                  job_nm: str,
                                  start_dttm: dt.datetime,
                                  end_dttm: dt.datetime,
                                  targetname: str,
                                  scope: str,
                                  run_id: str,
                                  success_statuses: List[str]) -> bool:
    """Return True if an audit row exists for the given target/window with a 'successful' status.

    Used to decide whether uploads can be skipped (resume/recovery).
    This intentionally does NOT filter by STAGE, because historically some runs only wrote
    upload rows without stage markers; we treat any SUCCESS/SKIPPED row as evidence.
    """
    statuses = success_statuses or ["SUCCESS", "SKIPPED"]
    status_binds = {f"st{i}": s for i, s in enumerate(statuses)}
    status_in = ", ".join([f":st{i}" for i in range(len(statuses))])

    sql = f"""
        SELECT 1
          FROM {audit_table}
         WHERE workflow_nm = :w
           AND mapping_nm  = :m
           AND job_nm      = :j
           AND start_dttm  = :sd
           AND end_dttm    = :ed
           AND targetname  = :tn
           AND status      IN ({status_in})
    """

    if (scope or "").upper() == "SAME_RUN":
        sql += " AND run_id = :rid"

    params: Dict[str, Any] = dict(w=workflow_nm, m=mapping_nm, j=job_nm, sd=start_dttm, ed=end_dttm, tn=targetname)
    params.update(status_binds)
    if (scope or "").upper() == "SAME_RUN":
        params["rid"] = run_id

    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        return cur.fetchone() is not None
    finally:
        try:
            cur.close()
        except Exception:
            pass


def query_audit_latest_counts(conn,
                             audit_table: str,
                             workflow_nm: str,
                             mapping_nm: str,
                             job_nm: str,
                             start_dttm: dt.datetime,
                             end_dttm: dt.datetime,
                             stage_value: str,
                             scope: str,
                             run_id: str,
                             success_statuses: Optional[List[str]] = None) -> Tuple[int, int]:
    """Return (target_count, reject_count) from the most recent successful audit row for a stage.

    This is used to carry forward counts when a stage is resumed (skipped).
    """
    statuses = success_statuses or ["SUCCESS", "SKIPPED"]
    status_binds = {f"st{i}": s for i, s in enumerate(statuses)}
    status_in = ", ".join([f":st{i}" for i in range(len(statuses))])

    sql = f"""
        SELECT target_count, reject_count
          FROM {audit_table}
         WHERE workflow_nm = :w
           AND mapping_nm  = :m
           AND job_nm      = :j
           AND start_dttm  = :sd
           AND end_dttm    = :ed
           AND stage       = :stage
           AND status      IN ({status_in})
    """

    if (scope or "").upper() == "SAME_RUN":
        sql += " AND run_id = :rid"

    sql += " ORDER BY updated_dttm DESC"

    params: Dict[str, Any] = dict(
        w=workflow_nm, m=mapping_nm, j=job_nm, sd=start_dttm, ed=end_dttm, stage=stage_value
    )
    params.update(status_binds)
    if (scope or "").upper() == "SAME_RUN":
        params["rid"] = run_id

    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        row = cur.fetchone()
        if not row:
            return 0, 0
        return int(row[0] or 0), int(row[1] or 0)
    finally:
        try:
            cur.close()
        except Exception:
            pass


def normalize_filename_for_counts(filename: str) -> str:
    """Strip compression/encryption suffixes to map final artifact -> base extract/split filename."""
    f = filename or ""
    # encryption first
    for suf in (".gpg", ".pgp"):
        if f.lower().endswith(suf):
            f = f[: -len(suf)]
            break
    # compression
    for suf in (".gz", ".bz2", ".zip"):
        if f.lower().endswith(suf):
            f = f[: -len(suf)]
            break
    return f


def query_audit_counts_for_target_stage(conn,
                                       audit_table: str,
                                       workflow_nm: str,
                                       mapping_nm: str,
                                       job_nm: str,
                                       start_dttm: dt.datetime,
                                       end_dttm: dt.datetime,
                                       run_id: str,
                                       targetname: str,
                                       stage_value: str,
                                       success_statuses: Optional[List[str]] = None,
                                       scope: str = "SAME_RUN") -> Tuple[int, int]:
    """Fetch (target_count, reject_count) for a specific TARGETNAME+STAGE.

    Used to populate done-file counts when the run starts at UPLOAD (manual stage selection) and
    the pipeline does not re-run extract/split.
    """
    statuses = success_statuses or ["SUCCESS", "SKIPPED"]
    status_binds = {f"st{i}": s for i, s in enumerate(statuses)}
    status_in = ", ".join([f":st{i}" for i in range(len(statuses))])

    sql = f"""
        SELECT target_count, reject_count
          FROM {audit_table}
         WHERE workflow_nm = :w
           AND mapping_nm  = :m
           AND job_nm      = :j
           AND start_dttm  = :sd
           AND end_dttm    = :ed
           AND targetname  = :tn
           AND stage       = :stage
           AND status      IN ({status_in})
    """

    params: Dict[str, Any] = {
        "w": workflow_nm,
        "m": mapping_nm,
        "j": job_nm,
        "sd": start_dttm,
        "ed": end_dttm,
        "tn": targetname,
        "stage": stage_value,
    }
    params.update(status_binds)

    if (scope or "").upper() == "SAME_RUN":
        sql += " AND run_id = :rid"
        params["rid"] = run_id

    sql += " ORDER BY updated_dttm DESC"

    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        row = cur.fetchone()
        if not row:
            return 0, 0
        return int(row[0] or 0), int(row[1] or 0)
    finally:
        try:
            cur.close()
        except Exception:
            pass

def newrelic_post_metrics(
    nr_api_key: str,
    nr_api_endpoint: str,
    metrics: Dict[str, Any]
):
    
    def fmt_dt(dt):
        return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else None
        
    if not nr_api_key:
        log_message("[NEWRELIC] enabled but nr_api_key not provided; skipping.")
        return
    try:
        header = {
            'Content-Type': 'application/json',
            'X-Insert-Key': nr_api_key
        }
        def_newrelic_data = {
            'eventType': 'BIDAY1_RPT',
            'workflow_nm': metrics.get("workflow_nm"),
            'mapping_nm': metrics.get("mapping_nm"),
            'job_nm': metrics.get("job_nm"),
            'run_id': metrics.get("run_id"),
            'targetname': metrics.get("targetname"),
            'status': metrics.get("status"),
            'stage': metrics.get("stage"),
            'target_count': metrics.get("target_count", 0),
            'reject_count': metrics.get("reject_count", 0),
            'start_dttm': fmt_dt(metrics.get("start_dttm")),
            'end_dttm': fmt_dt(metrics.get("end_dttm")),
            'timestamp': int(time.time()),
            'error_msg': metrics.get("error_msg")
        }
        
        newrelic_df = pd.DataFrame([def_newrelic_data])
        json_body = newrelic_df.to_json(orient='records')
        response = requests.post(url=nr_api_endpoint, data=json_body, headers=header)
        
        status = response.status_code
        body = response.text or ""

        log_message(
            "[NEWRELIC] metrics posted status={} body={}".format(
            status,
            body[:200]
        ))
        
    except Exception as e:
        log_message("[NEWRELIC] failed to post metrics: {}".format(str(e)))

def append_stage_audit_row(buffer: List[Dict[str, Any]],
                           newrelic_cfg: dict,
                           nr_api_key: str,
                           nr_api_endpoint: str,
                           job_nm: str,
                           mapping_nm: str,
                           workflow_nm: str,
                           start_dttm: dt.datetime,
                           end_dttm: dt.datetime,
                           run_id: str,
                           status: str,
                           targetname: str,
                           stage_label: str,
                           reject_count: int = 0,
                           target_count: int = 0,
                           error_msg: str = "") -> None:
                               
    if bool(newrelic_cfg.get("enabled", True)):
        metrics = {"job_nm": job_nm,
                   "mapping_nm": mapping_nm,
                   "workflow_nm": workflow_nm,
                   "start_dttm": start_dttm,
                   "end_dttm": end_dttm,
                   "run_id": run_id,
                   "status": status,
                   "targetname": targetname,
                   "stage": stage_label,
                   "reject_count": reject_count,
                   "target_count": target_count,
                   "error_msg": error_msg}
                   
        newrelic_post_metrics(nr_api_key, nr_api_endpoint, metrics)
    
    buffer.append({
        "job_nm": job_nm,
        "mapping_nm": mapping_nm,
        "workflow_nm": workflow_nm,
        "start_dttm": start_dttm,
        "end_dttm": end_dttm,
        "status": status,
        "run_id": run_id,
        "reject_count": int(reject_count),
        "target_count": int(target_count),
        "targetname": targetname,
        "stage": stage_to_audit_value(stage_label, ok=(status in ("SUCCESS", "SKIPPED"))),
        "error_msg": truncate_error_message(error_msg or ""),
    })


def flush_audit_buffer(conn, audit_table: str, buffer: List[Dict[str, Any]], context: str) -> None:
    if not buffer:
        return
    log_message("[AUDIT] bulk merge rows={} table={} context={}".format(len(buffer), audit_table, context))
    bulk_merge_audit_rows(conn, audit_table, buffer)
    buffer[:] = []


# ============================================================================
# AWS Profiles + S3 Client Routing
# ============================================================================
def load_aws_profiles(file_path: str) -> Dict[str, Any]:
    log_message("[AWS_PROFILES] loading file_path={}".format(file_path))
    if not file_path or not os.path.isfile(file_path):
        log_message("[AWS_PROFILES][ERROR] file not found: {}".format(file_path))
        raise ConfigError("aws_creds.file_path must point to aws_profiles.json with non-empty 'profiles' list")

    try:
        with open(file_path, "r", encoding="utf-8") as file_handle:
            data = json.load(file_handle)
    except Exception as exc:
        log_message("[AWS_PROFILES][ERROR] failed to read/parse JSON file={} err={}".format(file_path, exc))
        raise

    if isinstance(data, list):
        log_message("[AWS_PROFILES] detected list-only format; normalizing to {'profiles':[...]} structure")
        data = {"profiles": data}
    elif isinstance(data, dict) and "profiles" not in data:
        if {"aws_access_key_id", "aws_secret_access_key", "region"}.issubset(set(data.keys())):
            log_message("[AWS_PROFILES] detected single-profile shorthand; normalizing to {'profiles':[...]} structure")
            data = {"profiles": [data]}
        else:
            log_message("[AWS_PROFILES][ERROR] invalid format keys={}".format(list(data.keys())))
            raise ConfigError("aws_creds.file_path must be JSON with 'profiles' list")

    if "profiles" not in data or not isinstance(data["profiles"], list) or not data["profiles"]:
        log_message("[AWS_PROFILES][ERROR] 'profiles' missing/empty in file={}".format(file_path))
        raise ConfigError("aws_creds.file_path must point to aws_profiles.json with non-empty 'profiles' list")

    data.setdefault("default_profile", "")

    seen_names = set()
    for index, profile in enumerate(data["profiles"]):
        if not isinstance(profile, dict):
            raise ConfigError("aws_profiles.profiles[{}] must be an object".format(index))
        if not profile.get("name"):
            profile["name"] = "profile_{}".format(index + 1)

        for required_key in ("region", "aws_access_key_id", "aws_secret_access_key"):
            if not profile.get(required_key):
                raise ConfigError("aws_profiles.profiles[{}] missing required '{}'".format(index, required_key))

        profile.setdefault("session_token", "")
        profile.setdefault("buckets", [])
        if not isinstance(profile["buckets"], list):
            raise ConfigError("aws_profiles.profiles[{}].buckets must be a list".format(index))

        if profile["name"] in seen_names:
            profile["name"] = "{}_{}".format(profile["name"], index + 1)
        seen_names.add(profile["name"])

        log_message("[AWS_PROFILES] loaded profile idx={} name={} region={} buckets_mapped={}".format(
            index, profile.get("name"), profile.get("region"), len(profile.get("buckets", []) or [])
        ))

    log_message("[AWS_PROFILES] total_profiles={} default_profile={}".format(len(data.get("profiles", [])), data.get("default_profile") or ""))
    return data


def build_bucket_to_profile_map(aws_profiles: Dict[str, Any]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for profile in aws_profiles.get("profiles", []):
        profile_name = profile.get("name")
        for bucket_name in (profile.get("buckets", []) or []):
            if profile_name and bucket_name:
                mapping[bucket_name] = profile_name
    return mapping


def create_s3_client_from_profile(profile: Dict[str, Any]):
    kwargs = dict(
        region_name=profile["region"],
        aws_access_key_id=profile["aws_access_key_id"],
        aws_secret_access_key=profile["aws_secret_access_key"],
    )
    if profile.get("session_token"):
        kwargs["aws_session_token"] = profile.get("session_token")
    return boto3.client("s3", **kwargs)


def build_s3_client_router(aws_profiles: Dict[str, Any]):
    profiles = aws_profiles.get("profiles", [])
    default_name = aws_profiles.get("default_profile", "") or (profiles[0].get("name") if profiles else "")
    if not default_name:
        raise ConfigError("aws_profiles must have default_profile or at least one profile")

    profile_by_name: Dict[str, Dict[str, Any]] = {p.get("name"): p for p in profiles if p.get("name")}
    bucket_to_profile = build_bucket_to_profile_map(aws_profiles)

    cache: Dict[str, Any] = {}

    def get_client_for_profile(profile_name: str):
        if profile_name not in profile_by_name:
            raise ConfigError("No aws profile found with name='{}'".format(profile_name))
        if profile_name not in cache:
            cache[profile_name] = create_s3_client_from_profile(profile_by_name[profile_name])
        return cache[profile_name]

    def get_client_for_bucket(bucket: str, explicit_profile: Optional[str] = None):
        profile_name = explicit_profile or bucket_to_profile.get(bucket) or default_name
        return get_client_for_profile(profile_name)

    return get_client_for_bucket, get_client_for_profile


def parse_s3_uri(s3_uri: str) -> Tuple[str, str]:
    if not s3_uri.startswith("s3://"):
        raise ConfigError("Invalid s3 uri: {}".format(s3_uri))
    no_scheme = s3_uri[len("s3://"):]
    parts = no_scheme.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket, prefix.lstrip("/")


def s3_object_exists(s3_client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def upload_file_to_s3_with_policy(s3_client,
                                 local_path: str,
                                 target_s3_path: str,
                                 key_template: str,
                                 tokens: Dict[str, str],
                                 exists_behavior: str,
                                 retry_cfg: Dict[str, Any]) -> Tuple[str, str, str]:
    bucket, prefix = parse_s3_uri(target_s3_path)

    
    filename = os.path.basename(local_path)

    template_tokens = dict(tokens)
    template_tokens["filename"] = filename

    relative_key = render_template(key_template, template_tokens).lstrip("/")
    key = "{}/{}".format(prefix.rstrip("/"), relative_key) if prefix else relative_key

    behavior = (exists_behavior or "skip").strip().lower()
    if behavior not in ("skip", "overwrite", "fail"):
        raise ConfigError("target.exists_behavior must be skip|overwrite|fail")

    # Trace: show how partition/key is derived
    try:
        _tok_preview = {
            "yyyymmdd": template_tokens.get("yyyymmdd"),
            "yyyymmdd_start": template_tokens.get("yyyymmdd_start"),
            "yyyymmdd_end": template_tokens.get("yyyymmdd_end"),
            "yyyymm": template_tokens.get("yyyymm"),
            "hhmm": template_tokens.get("hhmm"),
            "filename": template_tokens.get("filename"),
            "part_idx": template_tokens.get("part_idx"),
            "total_parts": template_tokens.get("total_parts"),
        }
    except Exception:
        _tok_preview = {}

    log_message("[S3_UPLOAD] prepare local={} -> s3://{}/{} behavior={} key_template='{}' rendered_relative='{}' tokens={}".format(
        local_path, bucket, key, behavior, key_template, relative_key, _tok_preview
    ))

    def perform_upload():
        s3_client.upload_file(local_path, bucket, key)

    if behavior == "overwrite":
        retry_with_backoff(perform_upload, retry_cfg, "S3 upload {}".format(filename))
        return bucket, key, "UPLOADED"

    exists = s3_object_exists(s3_client, bucket, key)
    if exists and behavior == "skip":
        return bucket, key, "SKIPPED"
    if exists and behavior == "fail":
        raise UploadError("Target already exists: s3://{}/{}".format(bucket, key))

    retry_with_backoff(perform_upload, retry_cfg, "S3 upload {}".format(filename))
    return bucket, key, "UPLOADED"


def download_file_from_s3(s3_client, bucket: str, key: str, local_path: str, retry_cfg: Dict[str, Any]) -> None:
    ensure_directory(os.path.dirname(local_path))

    def perform_download():
        s3_client.download_file(bucket, key, local_path)

    retry_with_backoff(perform_download, retry_cfg, "S3 download {}".format(os.path.basename(local_path)))


def list_s3_keys_under_prefix(s3_client, source_s3_path: str) -> List[str]:
    bucket, prefix = parse_s3_uri(source_s3_path)
    keys: List[str] = []
    continuation_token = None
    while True:
        request_kwargs = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            request_kwargs["ContinuationToken"] = continuation_token
        response = s3_client.list_objects_v2(**request_kwargs)
        for obj in response.get("Contents", []):
            key = obj.get("Key")
            if key:
                keys.append(key)
        if response.get("IsTruncated"):
            continuation_token = response.get("NextContinuationToken")
        else:
            break
    return keys


# ============================================================================
# File utilities: split / compress / encrypt
# ============================================================================
def parse_size_to_bytes(size_str: str, unit_map: Dict[str, int]) -> int:
    s = (size_str or "").strip().upper().replace(" ", "")
    match = re.match(r"^(\d+)([A-Z]+)$", s)
    if not match:
        raise ConfigError("Invalid size '{}'. Expected like '1GB' or '1 GB'".format(size_str))
    n = int(match.group(1))
    unit = match.group(2)
    if unit not in unit_map:
        raise ConfigError("Unknown size unit '{}'".format(unit))
    return n * int(unit_map[unit])


def data_rows_in_text_file(file_path: str, header_present: bool) -> int:
    lines = count_lines_wc(file_path)
    if header_present:
        return max(0, lines - 1)
    return max(0, lines)


def split_file_if_needed(input_path: str,
                         split_enabled: bool,
                         split_bytes: int,
                         include_header_in_parts: bool,
                         header_enabled: bool,
                         encoding: str,
                         line_ending: str,
                         first_part_ind: bool = False) -> List[Dict[str, Any]]:
   
    def _single_part(path: str) -> List[Dict[str, Any]]:
        return [{
            "path": path,
            "part_idx": 1,
            "data_rows": data_rows_in_text_file(path, header_present=header_enabled),
        }]

    if not split_enabled:
        try:
            log_message("[SPLIT] enabled=False => no split. file={} size_bytes={}".format(
                os.path.basename(input_path), os.path.getsize(input_path) if os.path.exists(input_path) else -1
            ))
        except Exception:
            pass
        return _single_part(input_path)

    file_size = os.path.getsize(input_path)

    try:
        log_message("[SPLIT] enabled=True file={} size_bytes={} limit_bytes={} first_part_ind={} include_header_in_parts={} header_enabled={}".format(
            os.path.basename(input_path), file_size, split_bytes, bool(first_part_ind), bool(include_header_in_parts), bool(header_enabled)
        ))
    except Exception:
        pass

    if file_size <= split_bytes:
        if not first_part_ind:
            out = _single_part(input_path)
            try:
                log_message("[SPLIT] no_split size_within_limit. outputs={}".format([os.path.basename(x['path']) for x in out]))
            except Exception:
                pass
            return out

        base_dir = os.path.dirname(input_path)
        filename = os.path.basename(input_path)
        name, ext = os.path.splitext(filename)

        part1_name = f"{name}_1{ext}"
        part1_path = os.path.join(base_dir, part1_name)

        if os.path.exists(part1_path):
            try:
                os.remove(part1_path)
            except Exception:
                return _single_part(input_path)

        try:
            os.replace(input_path, part1_path)  # atomic rename when possible
        except Exception:
            return _single_part(input_path)

        out = _single_part(part1_path)
        try:
            log_message("[SPLIT] size_within_limit but first_part_ind=True => renamed_to={} outputs={}".format(
                os.path.basename(part1_path), [os.path.basename(x['path']) for x in out]
            ))
        except Exception:
            pass
        return out

    base_dir = os.path.dirname(input_path)
    filename = os.path.basename(input_path)
    name, ext = os.path.splitext(filename)

    header_line = None
    parts_meta: List[Dict[str, Any]] = []
    part_idx = 1
    current_bytes = 0
    current_rows = 0
    out_handle = None

    with open(input_path, "r", encoding=encoding, newline="") as fin:
        if header_enabled:
            header_line = fin.readline()
            if header_line and not header_line.endswith(line_ending):
                header_line = header_line.rstrip("\r\n") + line_ending

        def open_new_part():
            nonlocal out_handle, current_bytes, current_rows, part_idx
            if out_handle:
                out_handle.close()

            part_name = "{}_{}{}".format(name, part_idx, ext)
            part_path = os.path.join(base_dir, part_name)
            out_handle = open(part_path, "w", encoding=encoding, newline="")
            current_bytes = 0
            current_rows = 0

            if header_enabled and include_header_in_parts and header_line is not None:
                out_handle.write(header_line)
                current_bytes += len(header_line.encode(encoding))

            parts_meta.append({"path": part_path, "part_idx": part_idx, "data_rows": 0})
            part_idx += 1

        open_new_part()

        for line in fin:
            if line and not line.endswith(line_ending):
                line = line.rstrip("\r\n") + line_ending
            line_bytes = len(line.encode(encoding))

            if current_bytes + line_bytes > split_bytes:
                parts_meta[-1]["data_rows"] = int(current_rows)
                open_new_part()

            out_handle.write(line)
            current_bytes += line_bytes
            current_rows += 1

        if parts_meta:
            parts_meta[-1]["data_rows"] = int(current_rows)

    if out_handle:
        out_handle.close()

    try:
        os.remove(input_path)
    except Exception:
        pass

    try:
        log_message("[SPLIT] split_completed parts={} outputs={}".format(
            len(parts_meta), [os.path.basename(x['path']) for x in parts_meta]
        ))
    except Exception:
        pass

    return parts_meta



def compression_extension(comp_type: str) -> str:
    t = (comp_type or "").lower().strip()
    if t == "gzip":
        return ".gz"
    if t == "bz2":
        return ".bz2"
    if t == "zip":
        return ".zip"
    raise Exception("compression.type must be gzip|bz2|zip")


def compress_file(input_path: str, compression_cfg: Dict[str, Any], cfg: Dict[str, Any]) -> str:
    
    comp_type = (require_config(compression_cfg, "type", "compression") or "").lower().strip()
    ext = compression_extension(comp_type)
    table_to_s3cfg = require_config(cfg, "table_to_s3_config", "config")
    file_cfg = require_config(table_to_s3cfg,  "file","table_to_s3_config")
    output_dir = require_config(file_cfg, "output_dir", "file")
    filename = os.path.basename(input_path)
    
    compressed_output_dir = os.path.join(output_dir, "compressed")
    ensure_directory(compressed_output_dir)
    out_path = os.path.join(compressed_output_dir, filename + ext)


    if comp_type == "gzip":
        with open(input_path, "rb") as fin, gzip.open(out_path, "wb") as fout:
            shutil.copyfileobj(fin, fout)
        #os.remove(input_path)
        return out_path

    if comp_type == "bz2":
        with open(input_path, "rb") as fin, bz2.open(out_path, "wb") as fout:
            shutil.copyfileobj(fin, fout)
        #os.remove(input_path)
        return out_path

    if comp_type == "zip":
        arcname = os.path.basename(input_path)
        with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.write(input_path, arcname=arcname)
        #os.remove(input_path)
        return out_path


def gpg_encrypt_file(input_path: str, encryption_cfg: Dict[str, Any], retry_cfg: Dict[str, Any], cfg: Dict[str, Any]) -> str:
    gpg_bin = require_config(encryption_cfg, "gpg_bin", "encryption")
    recipient = require_config(encryption_cfg, "recipient", "encryption")
    gpg_home = get_config(encryption_cfg, "gpg_home", "")

    table_to_s3cfg = require_config(cfg, "table_to_s3_config", "config")
    file_cfg = require_config(table_to_s3cfg,  "file","table_to_s3_config")
    output_dir = require_config(file_cfg, "output_dir", "file")
    filename = os.path.basename(input_path)
    
    encrypted_output_dir = os.path.join(output_dir, "encrypted")
    ensure_directory(encrypted_output_dir)
    out_path = os.path.join(encrypted_output_dir, filename + ".gpg")


    env = os.environ.copy()
    if gpg_home:
        ensure_directory(gpg_home)
        env["GNUPGHOME"] = gpg_home

    cmd = [gpg_bin, "--batch", "--yes", "--trust-model", "always", "-o", out_path, "-r", recipient, "--encrypt", input_path]

    def run_gpg():
        rc, _out, err = run_command_capture_output(cmd, env=env)
        if rc != 0:
            raise EncryptError("gpg encrypt failed rc={} err={}".format(rc, (err or "").strip()))
        return True

    retry_with_backoff(run_gpg, retry_cfg, "GPG encrypt {}".format(os.path.basename(input_path)))

    #os.remove(input_path)
    return out_path


def floor_to_interval_boundary(ts: dt.datetime, interval_minutes: int, granularity: str) -> dt.datetime:
    gran = (granularity or "").lower().strip()
    if gran == "daily":
        return dt.datetime(ts.year, ts.month, ts.day, 0, 0, 0)

    ts0 = ts.replace(second=0, microsecond=0)
    if interval_minutes <= 0:
        return ts0

    minutes_since_midnight = ts0.hour * 60 + ts0.minute
    floored_minutes = (minutes_since_midnight // interval_minutes) * interval_minutes
    floored_hour = floored_minutes // 60
    floored_minute = floored_minutes % 60
    return ts0.replace(hour=floored_hour, minute=floored_minute, second=0, microsecond=0)


def parse_manual_start_end(load_cfg: Dict[str, Any]) -> Tuple[dt.datetime, dt.datetime]:
    start_s = require_config(load_cfg, "start", "load")
    end_s = require_config(load_cfg, "end", "load")

    def parse_any(s: str) -> dt.datetime:
        s = (s or "").strip()
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
            try:
                return dt.datetime.strptime(s, fmt)
            except Exception:
                pass
        raise ConfigError("Invalid load start/end format. Use 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'")

    return parse_any(start_s), parse_any(end_s)


def build_windows(load_cfg, cfg_row):
    """
    Build execution windows as (start_dt, end_dt_exclusive, tokens)

    Supports load.granularity:
      - adhoc   : always current day only (local timezone), one window
      - daily   : day windows
      - weekly  : week windows clipped to start/end (configurable week_start)
      - monthly : calendar-month windows clipped to start/end (handles 28/29/30/31 day months)
      - hourly  : hour windows
      - minute  : interval-minute windows

    Notes:
      - end_dt in each window is EXCLUSIVE.
    """

    def _floor_to_day(d: dt.datetime) -> dt.datetime:
        return dt.datetime(d.year, d.month, d.day, 0, 0, 0)

    def _first_of_month(d: dt.datetime) -> dt.datetime:
        return dt.datetime(d.year, d.month, 1, 0, 0, 0)

    def _add_months(d: dt.datetime, months: int) -> dt.datetime:
        # Calendar-safe month add: moves between month boundaries (1st of month)
        y = d.year + (d.month - 1 + months) // 12
        m = (d.month - 1 + months) % 12 + 1
        return dt.datetime(y, m, 1, 0, 0, 0)

    def _start_of_week(d: dt.datetime, week_start: int) -> dt.datetime:
        """
        week_start: 0=Monday ... 6=Sunday
        """
        day0 = _floor_to_day(d)
        delta = (day0.weekday() - week_start) % 7
        return day0 - dt.timedelta(days=delta)

    def _tokens_for_window(gran: str, cur: dt.datetime, w_end_excl: dt.datetime = None) -> dict:
        """
        Build tokens for templating.

        For weekly/monthly windows, pass w_end_excl so we can add:
          - yyyymmdd_start (window start day)
          - yyyymmdd_end   (window end day inclusive)  == (w_end_excl - 1 day)
        """
        tokens = {
            "yyyymmdd": cur.strftime("%Y%m%d"),
            "yyyy": cur.strftime("%Y"),
            "yyyymm": cur.strftime("%Y%m"),
            "mm": cur.strftime("%m"),
            "dd": cur.strftime("%d"),
            "hhmm": cur.strftime("%H%M"),
        }

        if gran == "weekly":
            iso_year, iso_week, _ = cur.isocalendar()
            tokens["iso_year"] = str(iso_year)
            tokens["iso_week"] = str(iso_week).zfill(2)

        if gran in ("monthly", "weekly") and w_end_excl is not None:
            end_inclusive_day = (w_end_excl - dt.timedelta(days=1))
            tokens["yyyymmdd_start"] = cur.strftime("%Y%m%d")
            tokens["yyyymmdd_end"] = end_inclusive_day.strftime("%Y%m%d")

        return tokens

    gran = (require_config(load_cfg, "granularity", "load") or "daily").lower()
    interval_minutes = int(get_config(load_cfg, "interval_minutes", 1440) or 1440)

    tz_name = get_config(load_cfg, "timezone", "America/New_York")
    tz = pytz.timezone(tz_name)

    # timezone-aware now, then drop tzinfo after we compute boundaries
    now_aware = dt.datetime.now(tz)
    now_local = now_aware.replace(tzinfo=None)

    load_type = (require_config(load_cfg, "type", "load") or "").upper()

    # ----------------------------
    # Determine start/end_inclusive
    # ----------------------------
    if load_type == "M":
        start, end_inclusive = parse_manual_start_end(load_cfg)
    else:
        previous_end = cfg_row.get("end_time")

        cap_mode = (get_config(load_cfg, "cap_mode", "") or "").lower().strip()

        if gran in ("daily", "weekly", "monthly", "adhoc"):
            # midnight-based cap
            if cap_mode == "completed_day":
                cap_end_excl = dt.datetime(now_local.year, now_local.month, now_local.day, 0, 0, 0)
            else:
                cap_end_excl = dt.datetime(now_local.year, now_local.month, now_local.day, 0, 0, 0) + dt.timedelta(days=1)
        else:
            cap_end_excl = floor_to_interval_boundary(now_local, interval_minutes, gran)

        lag_days = int(get_config(load_cfg, "lag_days", 0) or 0)
        lag_intervals = int(get_config(load_cfg, "lag_intervals", 0) or 0)

        if gran in ("daily", "weekly", "monthly", "adhoc") and lag_days > 0:
            cap_end_excl = cap_end_excl - dt.timedelta(days=lag_days)

        if gran in ("hourly", "minute") and lag_intervals > 0:
            cap_end_excl = cap_end_excl - dt.timedelta(minutes=interval_minutes * lag_intervals)

        if previous_end:
            start = previous_end
        else:
            bootstrap = (get_config(load_cfg, "bootstrap_start_mode", "previous_interval") or "previous_interval").lower().strip()
            if bootstrap == "today_only" and gran in ("daily", "adhoc"):
                start = cap_end_excl - dt.timedelta(days=1)
            else:
                start = cap_end_excl - dt.timedelta(minutes=interval_minutes)

        if start >= cap_end_excl:
            return []

        end_inclusive = cap_end_excl - dt.timedelta(seconds=1)

    # ----------------------------
    # Build windows (end exclusive)
    # ----------------------------
    windows = []

    if gran == "adhoc":
        cur = dt.datetime(now_local.year, now_local.month, now_local.day, 0, 0, 0)
        nxt = cur + dt.timedelta(days=1)
        windows.append((cur, nxt, _tokens_for_window(gran, cur)))

    elif gran == "daily":
        cur = dt.datetime(start.year, start.month, start.day, 0, 0, 0)
        last = dt.datetime(end_inclusive.year, end_inclusive.month, end_inclusive.day, 0, 0, 0)
        while cur <= last:
            nxt = cur + dt.timedelta(days=1)
            windows.append((cur, nxt, _tokens_for_window(gran, cur)))
            cur = nxt

    elif gran == "weekly":
        # Week windows clipped to [start_day, end_excl)
        week_start = int(get_config(load_cfg, "week_start", 0) or 0)  # 0=Mon..6=Sun

        start_day = _floor_to_day(start)
        end_excl = _floor_to_day(end_inclusive) + dt.timedelta(days=1)

        cur = _start_of_week(start_day, week_start)
        while cur < end_excl:
            nxt = cur + dt.timedelta(days=7)

            w_start = max(cur, start_day)
            w_end = min(nxt, end_excl)
            if w_start < w_end:
                windows.append((w_start, w_end, _tokens_for_window(gran, w_start, w_end)))

            cur = nxt

    elif gran == "monthly":
        # Calendar-month windows clipped to [start_day, end_excl)
        start_day = _floor_to_day(start)
        end_excl = _floor_to_day(end_inclusive) + dt.timedelta(days=1)

        cur = _first_of_month(start_day)
        while cur < end_excl:
            nxt = _add_months(cur, 1)

            w_start = max(cur, start_day)
            w_end = min(nxt, end_excl)
            if w_start < w_end:
                windows.append((w_start, w_end, _tokens_for_window(gran, w_start, w_end)))

            cur = nxt

    elif gran == "hourly":
        cur = start.replace(minute=0, second=0, microsecond=0)
        end_excl = end_inclusive.replace(minute=0, second=0, microsecond=0) + dt.timedelta(hours=1)
        while cur < end_excl:
            nxt = cur + dt.timedelta(hours=1)
            windows.append((cur, nxt, _tokens_for_window(gran, cur)))
            cur = nxt

    elif gran == "minute":
        cur = floor_to_interval_boundary(start, interval_minutes, gran)
        end_excl = floor_to_interval_boundary(end_inclusive, interval_minutes, gran) + dt.timedelta(minutes=interval_minutes)
        while cur < end_excl:
            nxt = cur + dt.timedelta(minutes=interval_minutes)
            windows.append((cur, nxt, _tokens_for_window(gran, cur)))
            cur = nxt

    else:
        raise ConfigError("load.granularity must be adhoc|daily|weekly|monthly|hourly|minute")

    max_windows = int(get_config(load_cfg, "max_windows_per_run", 0) or 0)
    if max_windows > 0 and len(windows) > max_windows:
        windows = windows[:max_windows]

    # Trace for SSH runs: computed extraction range + windows list
    try:
        # start/end_inclusive exist for both M and A
        log_message("[EXTRACT_RANGE] load.type={} granularity={} timezone={} start={} end_inclusive={} interval_minutes={}".format(
            load_type, gran, tz_name, start, end_inclusive, interval_minutes
        ))

        # Print all windows (bounded if huge)
        _max_print = int(get_config(load_cfg, "debug_max_windows_print", 500) or 500)
        for i, (ws, we, tok) in enumerate(windows[:_max_print], start=1):
            log_message("[WINDOW] idx={}/{} start={} end_excl={} tokens={}".format(i, len(windows), ws, we, tok))
        if len(windows) > _max_print:
            log_message("[WINDOW] ... truncated_print={} total_windows={}".format(_max_print, len(windows)))
    except Exception:
        pass

    return windows


def build_audit_range(load_cfg: Dict[str, Any], w_start: dt.datetime, w_end: dt.datetime) -> Tuple[dt.datetime, dt.datetime]:
    gran = (require_config(load_cfg, "granularity", "load") or "daily").lower()
    if gran == "daily":
        d = dt.datetime(w_start.year, w_start.month, w_start.day, 0, 0, 0)
        return d, d
    return w_start, w_end


# ============================================================================
# Oracle extraction
# ============================================================================
def csv_quoting_mode(value: str) -> int:
    v = (value or "").lower().strip()
    if v == "all":
        return csv.QUOTE_ALL
    if v == "minimal":
        return csv.QUOTE_MINIMAL
    if v == "none":
        return csv.QUOTE_NONE
    raise ConfigError("file.format.quoting must be all|minimal|none")


def build_oracle_select(source_cfg: Dict[str, Any]) -> Tuple[str, str, List[str]]:
    table = require_config(source_cfg, "table", "source")

    select_cols = get_config(source_cfg, "select_list", None)
    if select_cols is None:
        select_cols = get_config(source_cfg, "select_columns", [])

    headers: List[str] = []
    if select_cols:
        exprs = []
        for col_cfg in select_cols:
            expr = require_config(col_cfg, "expr", "source.select_list[]")
            alias = require_config(col_cfg, "alias", "source.select_list[]")
            exprs.append("{} AS {}".format(expr, alias))
            headers.append(alias)
        select_sql = "SELECT {} FROM {}".format(", ".join(exprs), table)
    else:
        select_sql = "SELECT * FROM {}".format(table)

    order_by = (get_config(source_cfg, "order_by", "") or "").strip()
    if not order_by:
        order_by = (get_config(source_cfg, "order_by_sql", "") or "").strip()

    return select_sql, order_by, headers


def build_oracle_window_sql(source_cfg: Dict[str, Any],
                            w_start: dt.datetime,
                            w_end: dt.datetime) -> Tuple[str, Dict[str, Any], List[str]]:
    base_sql, order_by, headers = build_oracle_select(source_cfg)

    col = get_config(source_cfg, "window_column", None) or get_config(source_cfg, "watermark_column", None)
    if not col:
        col = require_config(source_cfg, "watermark_column", "source")

    where_clause = "{} >= :watermark_start AND {} < :watermark_end".format(col, col)

    extra = (get_config(source_cfg, "extra_where", "") or "").strip()
    if not extra:
        extra = (get_config(source_cfg, "additional_filter_sql", "") or "").strip()

    sql = base_sql + " WHERE " + where_clause
    if extra:
        sql += " AND ({})".format(extra)
    if order_by:
        sql += " ORDER BY {}".format(order_by)

    binds = {"watermark_start": w_start, "watermark_end": w_end}
    return sql, binds, headers


def extract_oracle_stream_to_file(conn,
                                  source_cfg: Dict[str, Any],
                                  file_cfg: Dict[str, Any],
                                  runtime_cfg: Dict[str, Any],
                                  retry_cfg: Dict[str, Any],
                                  w_start: dt.datetime,
                                  w_end: dt.datetime,
                                  out_path: str) -> Tuple[int, int]:
    fmt = require_config(file_cfg, "format", "file")
    delimiter = require_config(fmt, "delimiter", "file.format")
    header_cfg = bool(get_config(fmt, "header", True))
    null_value = require_config(fmt, "null_value", "file.format", allow_empty_str=True)
    quoting = csv_quoting_mode(require_config(fmt, "quoting", "file.format"))

    quotechar = require_config(fmt, "quotechar", "file.format")
    if not isinstance(quotechar, str) or len(quotechar) != 1:
        raise ConfigError('"quotechar" must be a 1-character string')

    escapechar = get_config(fmt, "escapechar", None)
    if escapechar == "":
        escapechar = None
    if escapechar is not None and (not isinstance(escapechar, str) or len(escapechar) != 1):
        raise ConfigError('"escapechar" must be empty or a 1-character string')

    encoding = require_config(fmt, "encoding", "file.format")
    line_ending = normalize_line_ending(require_config(fmt, "line_ending", "file.format"))

    fetch_size = int(get_config(source_cfg, "fetch_size", 5000))
    count_mode = (get_config(runtime_cfg, "count_mode", "WC_L") or "WC_L").upper()
    count_excludes_header = bool(get_config(runtime_cfg, "count_excludes_header", True))

    ensure_directory(os.path.dirname(out_path))

    sql, binds, headers = build_oracle_window_sql(source_cfg, w_start, w_end)

    cursor = conn.cursor()
    cursor.arraysize = fetch_size

    reject_count = 0
    python_count = 0

    def perform_extract():
        nonlocal reject_count, python_count
        reject_count = 0
        python_count = 0

        cursor.execute(sql, **binds)

        log_message("[EXTRACT] cursor.arraysize={} fetch_size={} window_start={} window_end={}".format(fetch_size, fetch_size, w_start, w_end))

        if headers:
            header_row = headers
        else:
            header_row = [d[0] for d in cursor.description]

        first_batch = cursor.fetchmany(fetch_size)

        log_message("[EXTRACT] batch=1 fetched_rows={} fetch_size={}".format(len(first_batch), fetch_size))

        with open(out_path, "w", encoding=encoding, newline="") as file_handle:
            writer = csv.writer(
                file_handle,
                delimiter=delimiter,
                lineterminator=line_ending,
                quoting=quoting,
                quotechar=quotechar,
                escapechar=escapechar,
            )

            header_written = False
            if header_cfg or (not first_batch):
                writer.writerow(header_row)
                header_written = True

            for row in first_batch:
                try:
                    writer.writerow([(null_value if v is None else v) for v in row])
                    if count_mode == "PYTHON":
                        python_count += 1
                except Exception:
                    reject_count += 1

            batch_idx = 1

            while True:
                rows = cursor.fetchmany(fetch_size)
                if not rows:
                    break
                batch_idx += 1
                log_message("[EXTRACT] batch={} fetched_rows={} fetch_size={}".format(batch_idx, len(rows), fetch_size))
                for row in rows:
                    try:
                        writer.writerow([(null_value if v is None else v) for v in row])
                        if count_mode == "PYTHON":
                            python_count += 1
                    except Exception:
                        reject_count += 1

        if count_mode == "WC_L":
            lines = count_lines_wc(out_path)
            if header_written and count_excludes_header:
                lines = max(0, lines - 1)
            return int(lines), int(reject_count)

        if count_mode == "PYTHON":
            return int(python_count), int(reject_count)

        if count_mode == "NONE":
            return 0, int(reject_count)

        raise ConfigError("runtime.count_mode must be WC_L|PYTHON|NONE")

    try:
        return retry_with_backoff(perform_extract, retry_cfg, "Oracle extract {}".format(os.path.basename(out_path)))
    except Exception as exc:
        raise ExtractError("Oracle extract failed window {} - {}: {}".format(w_start, w_end, exc))
    finally:
        try:
            cursor.close()
        except Exception:
            pass



def strip_processing_extensions(filename: str) -> str:
    """Strip encryption/compression extensions to recover the base data filename."""
    name = filename
    for ext in (".gpg", ".pgp"):
        if name.lower().endswith(ext):
            name = name[: -len(ext)]
            break
    for ext in (".gz", ".bz2", ".zip"):
        if name.lower().endswith(ext):
            name = name[: -len(ext)]
            break
    return name

def infer_part_idx_from_filename(filename: str) -> int:
    """Infer split part index from a filename like base_12.txt(.gz|.gpg...). Returns 1 when not present."""
    base = strip_processing_extensions(os.path.basename(filename))
    # remove the data extension (.txt/.csv etc) for parsing
    stem, _ext = os.path.splitext(base)
    m = re.search(r"_(\d+)$", stem)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return 1
    return 1

def write_aggregate_done_file(done_cfg: Dict[str, Any],
                              tokens: Dict[str, str],
                              manifest_lines: List[Tuple[str, int]]) -> str:
    """Create ONE done file for the whole load (manifest style).

    Output format (one line per file/part):
        <filename> | <target_count>
    """
    output_dir = require_config(done_cfg, "output_dir", "done_file")
    name_template = require_config(done_cfg, "name_template", "done_file")
    ensure_directory(output_dir)

    # Render base name and ensure it ends with ".done"
    done_name = render_template(name_template, tokens)
    if not done_name.lower().endswith(".done"):
        done_name = done_name + ".done"

    done_path = os.path.join(output_dir, done_name)

    # Trace: show aggregated done file identity and tokens (safe subset)
    try:
        _tok = {
            "yyyymmdd_start": tokens.get("yyyymmdd_start"),
            "yyyymmdd_end": tokens.get("yyyymmdd_end"),
            "yyyymmdd": tokens.get("yyyymmdd"),
            "yyyymm": tokens.get("yyyymm"),
        }
        log_message("[DONE_AGG_BUILD] output_dir='{}' name_template='{}' rendered='{}' tokens={}".format(
            output_dir, name_template, done_name, _tok
        ))
    except Exception:
        pass

    # De-duplicate while preserving order (same file can appear if retries happen)
    seen = set()
    ordered: List[Tuple[str, int]] = []
    for fname, cnt in (manifest_lines or []):
        key = (fname or "").strip()
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        ordered.append((key, int(cnt or 0)))

    with open(done_path, "w", encoding="utf-8") as fh:
        for fname, cnt in ordered:
            fh.write("{} | {}\n".format(fname, cnt))

    try:
        preview = ["{} | {}".format(f, c) for (f, c) in ordered[:10]]
        log_message("[DONE_AGG_CONTENT] lines={} preview={}".format(len(ordered), preview))
    except Exception:
        pass

    return done_path

def build_dest_done_cfg(base_done_cfg: Dict[str, Any], dest: Dict[str, Any]) -> Dict[str, Any]:
    """Merge base done_file config with destination-level overrides."""
    merged = dict(base_done_cfg or {})
    dest_done = get_config(dest, "done_file", {}) or {}

    if "enabled" in dest_done:
        merged["enabled"] = dest_done.get("enabled")

    if dest_done.get("name_template"):
        merged["name_template"] = dest_done.get("name_template")

    if dest_done.get("output_dir"):
        merged["output_dir"] = dest_done.get("output_dir")

    merged_upload = dict(get_config(merged, "upload", {}) or {})
    dest_upload = get_config(dest_done, "upload", {}) or {}
    merged_upload.update(dest_upload)
    merged["upload"] = merged_upload

    return merged

# ============================================================================
# Purge
# ============================================================================
def purge_paths(purge_cfg: Dict[str, Any]) -> None:
    if not bool(get_config(purge_cfg, "enabled", False)):
        return
    rules = get_config(purge_cfg, "rules", []) or []
    now = now_eastern_naive()
    for rule in rules:
        base = require_config(rule, "path", "runtime.purge.rules[]")
        globs = get_config(rule, "include_globs", ["*"]) or ["*"]
        days = int(require_config(rule, "older_than_days", "runtime.purge.rules[]"))
        cutoff = now - dt.timedelta(days=days)
        if not os.path.isdir(base):
            continue
        for root, _, files in os.walk(base):
            for filename in files:
                if not any(fnmatch.fnmatch(filename, g) for g in globs):
                    continue
                path = os.path.join(root, filename)
                try:
                    mtime = dt.datetime.fromtimestamp(os.path.getmtime(path))
                    if mtime < cutoff:
                        os.remove(path)
                except Exception:
                    pass


# ============================================================================
# Targetname strategy
# ============================================================================
def build_targetname_for_audit(destination_name: str, filename: str, audit_targetname_template: str) -> str:
    return audit_targetname_template.format(dest=destination_name, filename=filename)


def build_local_stage_targetname(base_filename: str) -> str:
    # For stage-only auditing, no destination buckets apply. This stays inside TARGETNAME.
    return "{}{}".format(LOCAL_AUDIT_DEST, base_filename)


def _pick_local_artifact_for_destination(destination: Dict[str, Any],
                                        artifact_map: Dict[str, str]) -> str:
    """
    artifact_map keys expected:
      RAW, COMPRESSED, ENCRYPTED, COMPRESSED_ENCRYPTED

    Uses destination.upload_variant to pick the correct local artifact.
    """
    import os
    import traceback

    try:
        variant = (require_config(destination, "upload_variant", "target.destinations[]")).upper().strip()
        if variant not in ("RAW", "COMPRESSED", "ENCRYPTED", "COMPRESSED_ENCRYPTED"):
            raise ConfigError("target.destinations[].upload_variant must be RAW|COMPRESSED|ENCRYPTED|COMPRESSED_ENCRYPTED")

        chosen = (artifact_map or {}).get(variant, "")
        if not chosen:
            raise ConfigError(
                "Requested upload_variant={} but artifact is not available. available={}".format(
                    variant,
                    {k: os.path.basename(v) for k, v in (artifact_map or {}).items() if v}
                )
            )

        if not os.path.isfile(chosen):
            raise FrameworkError("Chosen artifact does not exist on disk: {}".format(chosen))

        return chosen

    except Exception as exc:
        # checks & balances: log rich context, then re-raise (no behavior change)
        try:
            tb = traceback.format_exc()
            dest_name = destination.get("name", "")
            dest_s3 = destination.get("s3_path", "")
            dest_variant = destination.get("upload_variant", "")
            avail = {k: os.path.basename(v) for k, v in (artifact_map or {}).items() if v}
            log_message("[ERROR] _pick_local_artifact_for_destination failed dest_name={} dest_s3_path={} "
                        "upload_variant={} available={} err={} \n{}".format(
                            dest_name, dest_s3, dest_variant, avail, str(exc), tb
                        ))
        except Exception:
            pass
        raise


def upload_artifact_to_destinations(
    get_client_for_bucket_fn,
    destinations: List[Dict[str, Any]],
    artifact_map: Dict[str, str],
    tokens: Dict[str, str],
    retry_cfg: Dict[str, Any],
    max_workers: int = 8,
    # Needed so DONE counts can be computed correctly (from the chosen data file)
    file_cfg: Optional[Dict[str, Any]] = None,
    compression_cfg: Optional[Dict[str, Any]] = None,
    encryption_cfg: Optional[Dict[str, Any]] = None,
    # Base done config (global defaults like output_dir, etc.). Destination overrides enablement/pattern/upload.
    base_done_cfg: Optional[Dict[str, Any]] = None,
    # If provided, DONE files are NOT created per file/window. Instead we append (filename, target_count)
    # to this collector and the caller creates ONE done file per load.
    aggregate_done_manifest_by_dest: Optional[Dict[str, List[Tuple[str, int]]]] = None,
) -> List[Tuple[str, str, str, str]]:
    """
    Upload to multiple S3 destinations in parallel across destinations (threadpool).

    Destination-scoped DONE config (under each destination):
      destinations[].done_file = {
        "enabled": true/false,
        "name_template": "...",            # optional override (local done filename pattern)
        "upload": {
          "enabled": true/false,           # upload done file or just generate locally
          "key_template": "...",           # REQUIRED if upload.enabled=true
          "exists_behavior": "overwrite|skip|fail"
        }
      }

    Ordering per destination:
      - Always upload DATA first.
      - Only if DATA action is success-like (UPLOADED/SKIPPED) => create/upload DONE (if enabled).

    Returns list of (destination_name, bucket, key, action).
      action may be:
        "UPLOADED"
        "SKIPPED"
        "UPLOADED;DONE=UPLOADED"
        "SKIPPED;DONE=CREATED_LOCAL"
        "UPLOADED;DONE=DISABLED"
        "error: ExceptionClass: message"
    """
    import os
    import threading
    import traceback
    from concurrent.futures import ThreadPoolExecutor, as_completed

    if not destinations:
        log_message("[WARN] upload_artifact_to_destinations called with destinations=0")
        return []

    if max_workers < 1:
        max_workers = 1

    client_cache: Dict[Tuple[str, Optional[str]], Any] = {}
    cache_lock = threading.Lock()

    def _log_error(prefix: str, exc: Exception, dest: Dict[str, Any], local_path: str = "", extra: str = ""):
        try:
            tb = traceback.format_exc()
            log_message("[ERROR] {} dest_name={} s3_path={} aws_profile={} upload_variant={} local_file={} {} err={} \n{}".format(
                prefix,
                dest.get("name", ""),
                dest.get("s3_path", ""),
                dest.get("aws_profile", ""),
                dest.get("upload_variant", ""),
                os.path.basename(local_path) if local_path else "",
                extra,
                str(exc),
                tb
            ))
        except Exception:
            pass

    def _get_s3_client_for_dest(dest: Dict[str, Any]):
        target_s3_path = require_config(dest, "s3_path", "target.destinations[]")
        explicit_profile = get_config(dest, "aws_profile", None)
        bucket, _ = parse_s3_uri(target_s3_path)
        cache_key = (bucket, explicit_profile)
        with cache_lock:
            s3_client = client_cache.get(cache_key)
            if s3_client is None:
                s3_client = get_client_for_bucket_fn(bucket, explicit_profile=explicit_profile)
                client_cache[cache_key] = s3_client
        return s3_client

    def _data_action_is_success(action: str) -> bool:
        a = (action or "").upper()
        return a in ("UPLOADED", "SKIPPED")

    def _build_dest_done_cfg(dest: Dict[str, Any]) -> Dict[str, Any]:
        # Start from base defaults (output_dir, etc.)
        merged = dict(base_done_cfg or {})
        dest_done = get_config(dest, "done_file", {}) or {}

        # destination can toggle enablement
        if "enabled" in dest_done:
            merged["enabled"] = dest_done.get("enabled")

        # destination can override name_template
        if dest_done.get("name_template"):
            merged["name_template"] = dest_done.get("name_template")

        # upload sub-config merge
        merged_upload = dict(get_config(merged, "upload", {}) or {})
        dest_upload = get_config(dest_done, "upload", {}) or {}
        merged_upload.update(dest_upload)
        merged["upload"] = merged_upload

        return merged

    def _upload_data_one(dest: Dict[str, Any]) -> Tuple[str, str, str, str, str]:
        """
        Returns: (destination_name, bucket, key, action, chosen_local_path)
        """
        destination_name = require_config(dest, "name", "target.destinations[]")
        target_s3_path   = require_config(dest, "s3_path", "target.destinations[]")
        key_template     = require_config(dest, "key_template", "target.destinations[]")
        exists_behavior  = require_config(dest, "exists_behavior", "target.destinations[]")

        s3_client = _get_s3_client_for_dest(dest)
        chosen_local_path = _pick_local_artifact_for_destination(dest, artifact_map)

        bucket2, key, action = upload_file_to_s3_with_policy(
            s3_client=s3_client,
            local_path=chosen_local_path,
            target_s3_path=target_s3_path,
            key_template=key_template,
            tokens=tokens,
            exists_behavior=exists_behavior,
            retry_cfg=retry_cfg,
        )
        return (destination_name, bucket2, key, action, chosen_local_path)

    def create_and_upload_done(dest: Dict[str, Any],
                               dest_name: str,
                               chosen_local_path: str,
                               data_action: str) -> str:
        """
        Returns "DONE=<action>" segment.
        """
        if not _data_action_is_success(data_action):
            log_message("[DONE_SKIP] dest={} reason='data upload not successful' data_action={}".format(dest_name, data_action))
            return "DONE=SKIPPED"

        done_cfg_eff = _build_dest_done_cfg(dest)
        if not bool(get_config(done_cfg_eff, "enabled", False)):
            return "DONE=DISABLED"

                # Default behavior: aggregate ONE DONE per load (caller creates it after ALL uploads).
        # We only append this artifact into the in-memory manifest for this destination.
        if aggregate_done_manifest_by_dest is None:
            # We intentionally removed the per-artifact done-file flow.
            raise ConfigError(
                "done_file.enabled=true requires aggregate_done_manifest_by_dest to be provided "
                "(per-artifact done file creation is disabled)"
            )

        # DONE manifest line should reflect the artifact uploaded to S3, but the COUNT must be computed
        # from a NON-encrypted local artifact (we do not have the private key to decrypt).
        uploaded_local_path = chosen_local_path
        uploaded_basename = os.path.basename(uploaded_local_path or "")

        tc = 0
        count_path = ""
        try:
            if file_cfg is None:
                raise ConfigError("file_cfg is required to compute done counts")

            # Compute counts from a NON-encrypted artifact:
            #   - COMPRESSED_ENCRYPTED -> COMPRESSED (.gz/.bz2/.zip)  (we can stream-decompress)
            #   - ENCRYPTED            -> RAW (.txt/.csv)             (we cannot decrypt)
            uploaded_lower = (uploaded_local_path or "").lower()
            if uploaded_lower.endswith((".gpg", ".pgp")):
                # Prefer explicit artifact_map mapping (most accurate).
                if os.path.normpath(uploaded_local_path) == os.path.normpath(artifact_map.get("COMPRESSED_ENCRYPTED", "")):
                    count_path = artifact_map.get("COMPRESSED") or ""
                elif os.path.normpath(uploaded_local_path) == os.path.normpath(artifact_map.get("ENCRYPTED", "")):
                    count_path = artifact_map.get("RAW") or ""

                # Robust fallback: derive expected RAW path from uploaded filename/location.
                if not count_path or not os.path.exists(count_path):
                    base_no_enc = normalize_filename_for_counts(uploaded_basename)  # strips .gpg/.pgp and compression suffixes
                    parent_dir = os.path.dirname(os.path.dirname(uploaded_local_path))  # .../<dataset>
                    count_path = find_first_existing_candidate([
                        artifact_map.get("RAW", ""),
                        os.path.join(parent_dir, "extract", base_no_enc),
                        os.path.join(parent_dir, "extract", base_no_enc + "*"),
                    ]) or ""
            else:
                # Not encrypted: we can count directly on the uploaded artifact (plain or compressed).
                count_path = uploaded_local_path

            if not count_path:
                raise FrameworkError("count_path could not be resolved for uploaded_local_path={}".format(uploaded_local_path))

            tc = int(compute_artifact_data_rows(count_path, file_cfg, compression_cfg or {}, encryption_cfg or {}) or 0)
        except Exception as cnt_exc:
            _log_error("DONE_COUNT_FAILED", cnt_exc, dest, local_path=uploaded_local_path, extra="count_path={}".format(count_path))
            tc = 0

        try:
            manifest_list = aggregate_done_manifest_by_dest.get(dest_name)
            if manifest_list is None:
                aggregate_done_manifest_by_dest[dest_name] = []
                manifest_list = aggregate_done_manifest_by_dest[dest_name]
            manifest_list.append((uploaded_basename, tc))
        except Exception as man_exc:
            _log_error("DONE_MANIFEST_APPEND_FAILED", man_exc, dest, local_path=chosen_local_path)

        return "DONE=DEFERRED"

    def _process_one(dest: Dict[str, Any]) -> Tuple[str, str, str, str]:
        dest_name = dest.get("name", "unknown")
        try:
            dn, bucket2, key, data_action, chosen_local_path = _upload_data_one(dest)
            done_seg = create_and_upload_done(dest, dn, chosen_local_path, data_action)
            final_action = "{};{}".format(data_action, done_seg) if done_seg else data_action
            return (dn, bucket2, key, final_action)
        except Exception as exc:
            _log_error("UPLOAD_DEST_FAILED", exc, dest, extra="artifact_map_keys={}".format(list((artifact_map or {}).keys())))
            return (dest_name, "", "", "error: {}: {}".format(exc.__class__.__name__, exc))

    results: List[Tuple[str, str, str, str]] = [("", "", "", "")] * len(destinations)
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        fut_to_idx = {ex.submit(_process_one, d): i for i, d in enumerate(destinations)}
        for fut in as_completed(fut_to_idx):
            idx = fut_to_idx[fut]
            try:
                results[idx] = fut.result()
            except Exception as exc:
                d = destinations[idx] if idx < len(destinations) else {}
                dn = d.get("name", "index:{}".format(idx))
                results[idx] = (dn, "", "", "error: {}: {}".format(exc.__class__.__name__, exc))

    return results


# ============================================================================
# Stage-aware helpers (resume checks)
# ============================================================================
def file_exists(path: str) -> bool:
    try:
        return os.path.isfile(path)
    except Exception:
        return False



def build_extract_stage_candidates(output_dir, base_filename, cfg):
    """
    Build candidate local artifact paths that prove 'Data Extract' has effectively completed.

    Data Extract is considered complete if ANY of these exist (driven by config):
      - raw extract: <name_template> (e.g., custdm_20260102.txt)
      - compressed:  <raw> + compression extension (e.g., .gz/.bz2/.zip) when compression.enabled=true
      - encrypted:   <raw> + (.gpg or .pgp) when encryption.enabled=true
      - compressed+encrypted: <raw><comp_ext><enc_ext> when both enabled
      - split variants when file.split.enabled=true (glob pattern with _* before original extension)
    """
    output_dir = output_dir or ""
    #base_path = os.path.join(output_dir, base_filename)

    table_to_s3cfg = (cfg or {}).get("table_to_s3_config") or {}

    file_cfg = (table_to_s3cfg.get("file") or {})
    split_cfg = (file_cfg.get("split") or {})
    split_enabled = bool(split_cfg.get("enabled", False))

    compression_cfg = (table_to_s3cfg.get("compression") or {})
    encryption_cfg = (table_to_s3cfg.get("encryption") or {})

    comp_enabled = bool(compression_cfg.get("enabled", False))
    enc_enabled = bool(encryption_cfg.get("enabled", False))

    comp_type = (compression_cfg.get("type") or "").strip().lower()
    comp_ext = ""
    if comp_enabled:
        if comp_type == "gzip":
            comp_ext = ".gz"
        elif comp_type == "bz2":
            comp_ext = ".bz2"
        elif comp_type == "zip":
            comp_ext = ".zip"
        else:
            # Unknown compression type; do not block resume logic
            comp_ext = ""

    enc_exts = [".gpg", ".pgp"] if enc_enabled else [""]

    candidates = []

    def add(prefix, suffix):
        candidates.append(prefix + suffix)

    # Base (non-split)
    if comp_ext:
        add(os.path.join(output_dir,'compressed', base_filename), comp_ext)
    if enc_enabled:
        for ex in enc_exts:
            add(os.path.join(output_dir,'encrypted', base_filename), ex)
            if comp_ext:
                add(os.path.join(output_dir,'encrypted', base_filename), comp_ext + ex)

    # Split part patterns
    if split_enabled:
        if comp_ext:
            root, ext = os.path.splitext(os.path.join(output_dir,'compressed', base_filename))
            part_prefix = root + "_*" + ext
            add(part_prefix, "")
            add(part_prefix, comp_ext)
        if enc_enabled:
            for ex in enc_exts:
                root, ext = os.path.splitext(os.path.join(output_dir,'encrypted', base_filename))
                part_prefix = root + "_*" + ext
                add(part_prefix, "")
                add(part_prefix, ex)
                if comp_ext:
                    add(part_prefix, comp_ext + ex)

    # de-dup
    out = []
    seen = set()
    for c in candidates:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def any_path_or_glob_exists(candidates):
    """Return True if any candidate exists (supports glob patterns)."""
    import glob
    for c in candidates or []:
        if any(ch in c for ch in ["*", "?", "["]):
            if glob.glob(c):
                return True
        else:
            if os.path.exists(c):
                return True
    return False


def find_first_existing_candidate(candidates):
    """Return the first existing path matched from candidates (supports glob patterns)."""
    import glob
    for c in candidates or []:
        if any(ch in c for ch in ["*", "?", "["]):
            matches = glob.glob(c)
            if matches:
                # Return a stable choice for logging/determinism
                return sorted(matches)[0]
        else:
            if os.path.exists(c):
                return c
    return None


def any_files_match(directory: str, pattern: str) -> bool:
    try:
        for fn in os.listdir(directory):
            if fnmatch.fnmatch(fn, pattern):
                return True
    except Exception:
        return False
    return False


def table_to_s3(conn,
                nr_api_key: str,
                nr_api_endpoint: str,
                cfg_row: Dict[str, Any],
                cfg: Dict[str, Any],
                cfg_table: str,
                audit_table: str,
                workflow_nm: str,
                mapping_nm: str,
                job_nm: str,
                run_id: str,
                manual_stage: str = "") -> int:
    """
    Mode: TABLE_TO_S3

    Notes:
      - Existing extract/split/compress/encrypt functionality preserved.
      - Upload stage:
          * destination execution_type: sequence|parallel
          * data then done ordering (done per destination)
          * missing local artifacts treated as error (stop/continue via runtime.on_error)
          * done file config under destination.done_file (via base_done_cfg merge)
          * done count computed from destination's chosen data file

    FIXES INCLUDED:
      (A) Split parts => part-specific DONE files with part-specific counts
      (B) Upload audit uses per-destination uploaded filename + per-destination target_count
    """
    import os
    import re
    import datetime as dt
    import traceback
    from pathlib import Path

    table_to_s3cfg = require_config(cfg, "table_to_s3_config", "config")
    load_cfg = require_config(table_to_s3cfg, "load", "table_to_s3_config")
    source_cfg = require_config(table_to_s3cfg, "source", "table_to_s3_config")
    file_cfg = require_config(table_to_s3cfg, "file", "table_to_s3_config")
    target_cfg = require_config(table_to_s3cfg, "target", "table_to_s3_config")
    runtime_cfg = require_config(table_to_s3cfg, "runtime", "table_to_s3_config")

    retry_cfg = build_retry_policy(cfg)

    compression_cfg = get_config(table_to_s3cfg, "compression", {"enabled": False}) or {"enabled": False}
    encryption_cfg = get_config(table_to_s3cfg, "encryption", {"enabled": False}) or {"enabled": False}
    done_cfg = get_config(table_to_s3cfg, "done_file", {"enabled": False}) or {"enabled": False}
    purge_cfg = get_config(cfg, "purge", {"enabled": False}) or {"enabled": False}
    newrelic_cfg = get_config(table_to_s3cfg, "newrelic", {"enabled": True}) or {"enabled": True}

    unit_map = get_config(
        table_to_s3cfg,
        "size_unit_map",
        {"B": 1, "KB": 1024, "MB": 1048576, "GB": 1073741824, "TB": 1099511627776}
    ) or {}

    # Targets (multi-destination or single)
    destinations = get_config(target_cfg, "destinations", None)
    if not destinations:
        destinations = [{
            "name": "default",
            "s3_path": require_config(target_cfg, "s3_path", "target"),
            "key_template": require_config(target_cfg, "key_template", "target"),
            "exists_behavior": require_config(target_cfg, "exists_behavior", "target"),
            "audit_status_on_skip": require_config(target_cfg, "audit_status_on_skip", "target"),
            "compression_enabled": bool(compression_cfg.get("enabled", False)),
            "encryption_enabled": bool(encryption_cfg.get("enabled", False)),
        }]
    audit_targetname_template = get_config(target_cfg, "audit_targetname_template", "{dest}:{filename}")

    # ------------------------------------------------------------------------
    # DONE file (NEW): one done file per load/run (not per window/file).
    # We collect (uploaded_filename, target_count) per destination while uploading.
    # ------------------------------------------------------------------------
    done_agg_performed_in_sequence = False  # set True when COX done agg completes in-sequence
    aggregate_done_enabled = bool(get_config(done_cfg, "enabled", False))
    done_manifest_by_dest: Dict[str, List[Tuple[str, int]]] = {}
    dest_overall_ok: Dict[str, bool] = {}
    if aggregate_done_enabled and not done_agg_performed_in_sequence:
        for _d in destinations:
            _dn = _d.get("name", "unknown")
            done_manifest_by_dest[_dn] = []
            dest_overall_ok[_dn] = True


    # AWS client routing
    aws_cfg = require_config(table_to_s3cfg, "aws_creds", "table_to_s3_config")
    aws_profiles = load_aws_profiles(require_config(aws_cfg, "file_path", "aws_creds"))
    get_client_for_bucket, _ = build_s3_client_router(aws_profiles)

    # Recovery configuration
    recovery_cfg = get_config(runtime_cfg, "recovery", {"enabled": True}) or {"enabled": True}
    recovery_enabled = bool(get_config(recovery_cfg, "enabled", True))
    resume_mode = (get_config(recovery_cfg, "resume_mode", "SKIP_SUCCESS") or "SKIP_SUCCESS").upper()
    recovery_scope = (get_config(recovery_cfg, "scope", "SAME_RUN") or "SAME_RUN").upper()
    success_statuses = get_config(recovery_cfg, "success_statuses", ["SUCCESS", "SKIPPED"]) or ["SUCCESS", "SKIPPED"]

    on_error = (require_config(runtime_cfg, "on_error", "runtime") or "stop").lower()

    output_dir = require_config(file_cfg, "output_dir", "file")
    ensure_directory(output_dir)

    extract_output_dir = os.path.join(output_dir, 'extract')
    compressed_output_dir = os.path.join(output_dir, 'compressed')
    encrypted_output_dir = os.path.join(output_dir, 'encrypted')

    windows = build_windows(load_cfg, cfg_row)
    if not windows:
        log_message("[WINDOWS] count=0 (no work)")
        return 0

    name_template = require_config(file_cfg, "name_template", "file")

    # Load type
    load_type = (require_config(load_cfg, "type", "load") or "").upper()

    # For load.type="M", force S3 dt= partition to the run date (America/New_York)
    run_partition_yyyymmdd = now_eastern_naive().strftime("%Y%m%d")

    # Manual stage selection
    stage_order = ["EXTRACT", "SPLIT", "COMPRESS", "ENCRYPT", "UPLOAD"]

    manual_stage_effective = (manual_stage or "").strip()
    if load_type == "M" and not manual_stage_effective:
        manual_stage_effective = (get_config(load_cfg, "manual_stage", "") or "").strip()
        if not manual_stage_effective:
            manual_stage_effective = (get_config(runtime_cfg, "manual_stage", "") or "").strip()

    manual_stage_norm = (manual_stage_effective or "").strip().upper()
    if manual_stage_norm in ("ALL", "ALL_STAGES", ""):
        start_stage_idx = 0
    else:
        if load_type != "M":
            raise ConfigError("manual_stage is only valid for load.type='M'")
        if manual_stage_norm not in stage_order:
            raise ConfigError("manual_stage must be one of {} or ALL_STAGES".format(stage_order))
        start_stage_idx = stage_order.index(manual_stage_norm)

    log_message("[MANUAL_STAGE] load_type={} requested='{}' start_from={}({})".format(
        load_type,
        manual_stage_effective or "ALL_STAGES",
        stage_order[start_stage_idx],
        start_stage_idx
    ))

    if load_type != "M":
        start_stage_idx = 0
        manual_stage_norm = ""

    # max_workers (CONFIG DRIVEN)
    pipeline_cfg = get_config(runtime_cfg, "pipeline", {}) or {}
    max_workers = int(get_config(pipeline_cfg, "max_workers", get_config(runtime_cfg, "pipeline_max_workers", 8) or 8) or 8)
    if max_workers < 1:
        max_workers = 1

    audit_buffer: List[Dict[str, Any]] = []
    overall_ok = True

    def flush(context: str):
        flush_audit_buffer(conn, audit_table, audit_buffer, context=context)

    def should_stop_on_error() -> bool:
        return (on_error or "stop").lower() == "stop"

    def log_exception(stage: str, exc: Exception, **ctx):
        try:
            tb = traceback.format_exc()
            log_message("[ERROR] stage={} ctx={} err={} \n{}".format(stage, ctx, str(exc), tb))
        except Exception:
            pass

    def fail_stage_and_audit(stage_label: str,
                             targetname: str,
                             exc: Exception,
                             audit_context: str,
                             audit_start: dt.datetime,
                             audit_end: dt.datetime):
        nonlocal overall_ok
        overall_ok = False
        append_stage_audit_row(
            audit_buffer, newrelic_cfg, nr_api_key, nr_api_endpoint, job_nm, mapping_nm, workflow_nm, audit_start, audit_end, run_id,
            status="FAILED",
            targetname=targetname,
            stage_label=stage_label,
            reject_count=0,
            target_count=0,
            error_msg=str(exc),
        )
        flush(audit_context)
        log_exception(stage_label, exc, workflow_nm=workflow_nm, mapping_nm=mapping_nm, job_nm=job_nm,
                      run_id=run_id, targetname=targetname, audit_start=str(audit_start), audit_end=str(audit_end))

    def list_matching(pattern: str) -> List[str]:
        import glob
        return sorted(glob.glob(os.path.join(extract_output_dir, pattern)))

    def comp_list_matching(pattern: str) -> List[str]:
        import glob
        return sorted(glob.glob(os.path.join(compressed_output_dir, pattern)))

    def encr_list_matching(pattern: str) -> List[str]:
        import glob
        return sorted(glob.glob(os.path.join(encrypted_output_dir, pattern)))

    # execution_type checks & balances
    execution_type = (get_config(target_cfg, "execution_type", "parallel") or "parallel").strip().lower()
    if execution_type not in ("parallel", "sequence"):
        raise ConfigError("target.execution_type must be 'parallel' or 'sequence'")
    for i, d in enumerate(destinations):
        et = (get_config(d, "execution_type", execution_type) or execution_type).strip().lower()
        if et != execution_type:
            raise ConfigError("Inconsistent execution_type across destinations: first='{}' dest[{}]='{}'".format(execution_type, i, et))

    # Defer uploads across all windows so "sequence" can do dest1 for ALL dates/files first.
    upload_tasks: List[Dict[str, Any]] = []

    def parallel_map_files(stage_label: str, fn, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Apply fn to each item dict {'path':..., 'part_idx':..., 'data_rows':...} and return same structure with updated path.
        """
        if not items:
            return []
        if max_workers <= 1 or len(items) <= 1:
            out = []
            for it in items:
                out.append(fn(it))
            return out

        log_message("[{}] parallel_enabled max_workers={} files={}".format(stage_label, max_workers, [os.path.basename(i["path"]) for i in items]))
        out: List[Dict[str, Any]] = []
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = [ex.submit(fn, it) for it in items]
            for f in as_completed(futs):
                out.append(f.result())
        out.sort(key=lambda x: int(x.get("part_idx") or 0))
        return out

    # precompute comp_ext safely
    comp_ext = ""
    if bool(get_config(compression_cfg, "enabled", False)):
        try:
            comp_ext = compression_extension(require_config(compression_cfg, "type", "compression"))
        except Exception:
            comp_ext = ""

    for (w_start, w_end, tokens) in windows:
        audit_start, audit_end = build_audit_range(load_cfg, w_start, w_end)

        target_count = 0
        reject_count = 0

        alt_audit_ranges: List[Tuple[dt.datetime, dt.datetime]] = []
        if (load_cfg.get("granularity", "daily") or "daily").lower() == "daily":
            alt_audit_ranges.append((w_start, w_end))
            alt_audit_ranges.append((w_start, w_start))

        base_filename = render_template(name_template, tokens)
        # Trace: show the extraction window and the rendered base filename
        log_message("[WINDOW_START] window_start={} window_end_excl={} audit_start={} audit_end={} base_filename='{}' tokens={}".format(
            w_start, w_end, audit_start, audit_end, base_filename, tokens
        ))
        if (not base_filename) or ("/" in base_filename) or ("\\" in base_filename) or re.search(r"[\s|]", base_filename):
            raise ConfigError("file.name_template rendered an invalid filename: '{}'".format(base_filename))

        local_targetname = build_local_stage_targetname(base_filename)

        fmt = require_config(file_cfg, "format", "file")
        encoding = require_config(fmt, "encoding", "file.format")
        line_ending = normalize_line_ending(require_config(fmt, "line_ending", "file.format"))
        header_enabled = bool(get_config(fmt, "header", True))

        split_cfg = get_config(file_cfg, "split", {"enabled": False}) or {"enabled": False}
        split_enabled = bool(get_config(split_cfg, "enabled", False))
        split_size = get_config(split_cfg, "size", "")
        first_part_ind = bool(get_config(split_cfg, "first_part_ind", True))
        split_bytes = parse_size_to_bytes(split_size, unit_map) if split_enabled else 0
        include_header_in_parts = bool(get_config(split_cfg, "include_header_in_parts", True))

        comp_enabled = bool(get_config(compression_cfg, "enabled", False))
        enc_enabled = bool(get_config(encryption_cfg, "enabled", False))

        extract_path = os.path.join(output_dir, 'extract', base_filename)

        run_extract = (start_stage_idx <= 0)
        run_split = (start_stage_idx <= 1)
        run_compress = (start_stage_idx <= 2)
        run_encrypt = (start_stage_idx <= 3)
        run_upload = (start_stage_idx <= 4)

        # ---------------------------
        # Stage 1: Data Extract
        # ---------------------------
        extracted_rowcount = None
        if run_extract:
            try:
                if recovery_enabled and query_audit_stage_completed_flexible(
                    conn=conn,
                    audit_table=audit_table,
                    workflow_nm=workflow_nm,
                    mapping_nm=mapping_nm,
                    job_nm=job_nm,
                    run_id=run_id,
                    stage_value="{} {}".format(STAGE_DATA_EXTRACT, STAGE_COMPLETED),
                    targetname=local_targetname,
                    window_start=audit_start,
                    window_end=audit_end,
                    alternate_ranges=alt_audit_ranges,
                    scope=recovery_scope,
                    success_statuses=success_statuses,
                ):
                    candidates = build_extract_stage_candidates(output_dir, base_filename, cfg)
                    found = find_first_existing_candidate(candidates)
                    if found:
                        log_message("[RESUME] Skipping Data Extract (audit completed and local artifact exists): {}".format(found))
                    else:
                        log_message("[RESUME] Data Extract audit completed but no local artifact found; re-running extract.")
                        target_count, reject_count = extract_oracle_stream_to_file(
                            conn=conn, source_cfg=source_cfg, file_cfg=file_cfg, runtime_cfg=runtime_cfg,
                            retry_cfg=retry_cfg, w_start=w_start, w_end=w_end, out_path=extract_path,
                        )
                        extracted_rowcount = int(target_count)
                        append_stage_audit_row(
                            audit_buffer, newrelic_cfg, nr_api_key, nr_api_endpoint, job_nm, mapping_nm, workflow_nm, audit_start, audit_end, run_id,
                            status="SUCCESS", targetname=base_filename, stage_label=STAGE_DATA_EXTRACT,
                            reject_count=reject_count, target_count=target_count
                        )
                        flush("TABLE_TO_S3:Data Extract Completed")
                else:
                    target_count, reject_count = extract_oracle_stream_to_file(
                        conn=conn, source_cfg=source_cfg, file_cfg=file_cfg, runtime_cfg=runtime_cfg,
                        retry_cfg=retry_cfg, w_start=w_start, w_end=w_end, out_path=extract_path,
                    )
                    extracted_rowcount = int(target_count)
                    append_stage_audit_row(
                        audit_buffer, newrelic_cfg, nr_api_key, nr_api_endpoint, job_nm, mapping_nm, workflow_nm, audit_start, audit_end, run_id,
                        status="SUCCESS", targetname=base_filename, stage_label=STAGE_DATA_EXTRACT,
                        reject_count=reject_count, target_count=target_count
                    )
                    flush("TABLE_TO_S3:Data Extract Completed")
            except Exception as exc:
                fail_stage_and_audit(STAGE_DATA_EXTRACT, base_filename, exc, "TABLE_TO_S3:Data Extract Failed", audit_start, audit_end)
                if should_stop_on_error():
                    break
                continue
        else:
            log_message("[MANUAL_STAGE] load.type=M start_stage={} => skipping Data Extract".format(manual_stage_norm or "ALL_STAGES"))

        # ---------------------------
        # Stage 2: Data Split (optional)
        # ---------------------------
        # Trace: list extraction outputs before any split
        try:
            if os.path.exists(extract_path):
                log_message("[EXTRACT_OUTPUT] file={} path={} size_bytes={} target_rows={}".format(
                    os.path.basename(extract_path), extract_path, os.path.getsize(extract_path), extracted_rowcount
                ))
            else:
                bn, be = os.path.splitext(base_filename)
                outs = list_matching("{}*{}".format(bn, be))
                if outs:
                    log_message("[EXTRACT_OUTPUT] files_found={}".format([os.path.basename(p) for p in outs]))
        except Exception:
            pass
        split_would_occur = False
        if split_enabled and os.path.exists(extract_path):
            try:
                if file_exists(extract_path):
                    split_would_occur = os.path.getsize(extract_path) > split_bytes or first_part_ind
                else:
                    bn, be = os.path.splitext(base_filename)
                    split_would_occur = len(list_matching("{}_*{}".format(bn, be))) > 0
            except Exception:
                split_would_occur = False

        parts_meta: List[Dict[str, Any]] = []
        if run_split and split_enabled and split_would_occur:
            try:
                split_stage_value = "{} {}".format(STAGE_DATA_SPLIT, STAGE_COMPLETED)
                bn, be = os.path.splitext(base_filename)
                split_pattern = "{}_*{}".format(bn, be)
                split_paths = list_matching(split_pattern)
                splitted_file_name_list = [Path(p).name for p in split_paths]

                split_completed = recovery_enabled and query_audit_stage_completed_flexible(
                    conn=conn,
                    audit_table=audit_table,
                    workflow_nm=workflow_nm,
                    mapping_nm=mapping_nm,
                    job_nm=job_nm,
                    run_id=run_id,
                    stage_value=split_stage_value,
                    alternate_targetnames=splitted_file_name_list,
                    window_start=audit_start,
                    window_end=audit_end,
                    alternate_ranges=alt_audit_ranges,
                    scope=recovery_scope,
                    success_statuses=success_statuses,
                )

                if split_completed and split_paths:
                    log_message("[RESUME] Skipping Data Split (audit completed and split parts exist): {}".format(split_pattern))
                    for i, p in enumerate(split_paths, start=1):
                        parts_meta.append({"path": p, "part_idx": i, "data_rows": data_rows_in_text_file(p, header_present=header_enabled)})
                else:
                    parts_meta = split_file_if_needed(
                        input_path=extract_path,
                        split_enabled=True,
                        split_bytes=split_bytes,
                        include_header_in_parts=include_header_in_parts,
                        header_enabled=header_enabled,
                        encoding=encoding,
                        line_ending=line_ending,
                        first_part_ind=first_part_ind,
                    )
                    for part in parts_meta:
                        append_stage_audit_row(
                            audit_buffer, newrelic_cfg, nr_api_key, nr_api_endpoint, job_nm, mapping_nm, workflow_nm, audit_start, audit_end, run_id,
                            status="SUCCESS",
                            targetname=os.path.basename(part["path"]),
                            stage_label=STAGE_DATA_SPLIT,
                            reject_count=0,
                            target_count=int(part.get("data_rows") or 0),
                        )
                    flush("TABLE_TO_S3:Data Split Completed")
            except Exception as exc:
                fail_stage_and_audit(STAGE_DATA_SPLIT, local_targetname, exc, "TABLE_TO_S3:Data Split Failed", audit_start, audit_end)
                if should_stop_on_error():
                    break
                continue

        # Build parts_meta for downstream stages if needed
        if not parts_meta:
            if split_enabled and os.path.exists(extract_path):
                bn, be = os.path.splitext(base_filename)
                split_paths = list_matching("{}_*{}".format(bn, be))
                if split_paths:
                    for i, p in enumerate(split_paths, start=1):
                        parts_meta.append({"path": p, "part_idx": i, "data_rows": data_rows_in_text_file(p, header_present=header_enabled)})
            if not parts_meta and os.path.exists(extract_path):
                rc = extracted_rowcount if extracted_rowcount is not None else data_rows_in_text_file(extract_path, header_present=header_enabled)
                parts_meta = [{"path": extract_path, "part_idx": 1, "data_rows": int(rc)}]

        # Trace: show the files that will feed the next stages (post split)
        try:
            log_message("[SPLIT_OUTPUT] parts={} files={}".format(
                len(parts_meta), [os.path.basename(p.get("path","")) for p in parts_meta]
            ))
        except Exception:
            pass

        # ---------------------------
        # Stage 3: Data Compress (optional) - parallel
        # ---------------------------
        if run_compress and comp_enabled:
            try:
                comp_ext = compression_extension(require_config(compression_cfg, "type", "compression"))


                # Resume: if compressed outputs already exist and audit completed, skip
                base_name, base_ext = os.path.splitext(base_filename)
                split_paths = list_matching("{}*{}".format(base_name,base_ext))
                comp_split_paths = comp_list_matching("{}*{}".format(base_name,comp_ext))
                split_file_name_list = [Path(p).name for p in comp_split_paths]

                enc_split_paths = encr_list_matching("{}*{}".format(base_name,'.gpg'))

                all_exists = (bool(comp_split_paths) or bool(enc_split_paths)) and ((all(file_exists(p) for p in comp_split_paths)) or (all(file_exists(p) for p in enc_split_paths)))
                compressed_output_dir = output_dir+'compressed'

                compress_stage_value = "{} {}".format(STAGE_DATA_COMPRESS, STAGE_COMPLETED)
                compress_completed = recovery_enabled and query_audit_stage_completed_flexible(
                    conn=conn,
                    audit_table=audit_table,
                    workflow_nm=workflow_nm,
                    mapping_nm=mapping_nm,
                    job_nm=job_nm,
                    run_id=run_id,
                    stage_value=compress_stage_value,
                    alternate_targetnames=split_file_name_list,
                    window_start=audit_start,
                    window_end=audit_end,
                    alternate_ranges=alt_audit_ranges,
                    scope=recovery_scope,
                        success_statuses=success_statuses,
                )
                if compress_completed and all_exists:
                    log_message("[RESUME] Skipping Data Compress (audit completed and compressed files exist)")
                    seen = set()
                    deduped_paths = []
                    for p in comp_split_paths:
                        if p not in seen:
                            deduped_paths.append(p)
                            seen.add(p)

                    parts_meta = []
                    for i, p in enumerate(deduped_paths, start=1):
                        rows = data_rows_in_text_file(p, header_present=header_enabled)
                        parts_meta.append({"path": p, "part_idx": i, "data_rows": rows})
                    # update parts_meta paths to compressed
                    new_parts = []
                    for p in parts_meta:
                        new_path = p["path"] if p["path"].endswith(comp_ext) else (p["path"] + comp_ext)
                        new_parts.append({"path": new_path, "part_idx": p["part_idx"], "data_rows": p.get("data_rows", 0)})
                    parts_meta = new_parts
                else:
                    log_message("[COMPRESS] start files={} max_workers={}".format([os.path.basename(p["path"]) for p in parts_meta], max_workers))
                    seen = set()
                    deduped_paths = []
                    for p in split_paths:
                        if p not in seen:
                            deduped_paths.append(p)
                            seen.add(p)

                    parts_meta = []
                    for i, p in enumerate(deduped_paths, start=1):
                        rows = data_rows_in_text_file(p, header_present=header_enabled)
                        parts_meta.append({"path": p, "part_idx": i, "data_rows": rows})

                    def do_one(it: Dict[str, Any]) -> Dict[str, Any]:
                        part_idx = int(it.get("part_idx") or 1)
                        in_path = it["path"]
                        log_message("[COMPRESS] file_start part_idx={} file={}".format(part_idx, os.path.basename(in_path)))
                        outp = compress_file(in_path, compression_cfg, cfg)
                        log_message("[COMPRESS] file_done part_idx={} output={}".format(part_idx, os.path.basename(outp)))
                        return {"path": outp, "part_idx": part_idx, "data_rows": it.get("data_rows", 0)}
                            
                    parts_meta = parallel_map_files("COMPRESS", do_one, parts_meta)

                    # Audit one row per artifact with accurate counts
                    for _pm in parts_meta:
                        _tname = build_local_stage_targetname(os.path.basename(_pm["path"]))
                        try:
                            _tc = int(compute_artifact_data_rows(_pm["path"], file_cfg, compression_cfg, encryption_cfg) or 0)
                        except Exception as _cnt_exc:
                            log_message("[WARN] compress count compute failed file={} err={}".format(os.path.basename(_pm["path"]), _cnt_exc))
                            _tc = int(_pm.get("data_rows") or 0)

                        append_stage_audit_row(
                            audit_buffer, newrelic_cfg, nr_api_key, nr_api_endpoint, job_nm, mapping_nm, workflow_nm, audit_start, audit_end, run_id,
                            status="SUCCESS", targetname=_tname, stage_label=STAGE_DATA_COMPRESS,
                            reject_count=0, target_count=_tc
                        )
                    flush("TABLE_TO_S3:Data Compress Completed")
            except Exception as exc:
                overall_ok = False
                append_stage_audit_row(
                    audit_buffer, newrelic_cfg, nr_api_key, nr_api_endpoint, job_nm, mapping_nm, workflow_nm, audit_start, audit_end, run_id,
                    status="FAILED", targetname=local_targetname, stage_label=STAGE_DATA_COMPRESS,
                    reject_count=0, target_count=0, error_msg=str(exc)
                )
                flush("TABLE_TO_S3:Data Compress Failed")
                log_message("[ERROR] Data Compress failed file={} err={}".format(base_filename, exc))
                if should_stop_on_error():
                    break
                continue
        else:
            if not run_compress and comp_enabled:
                log_message("[MANUAL_STAGE] start_stage={} => skipping Data Compress".format(manual_stage_norm))

        # Trace: list outputs after compress stage (or passthrough if not enabled)
        try:
            log_message("[COMPRESS_OUTPUT] comp_enabled={} files={}".format(
                bool(comp_enabled), [os.path.basename(p.get("path","")) for p in parts_meta]
            ))
        except Exception:
            pass


        # ---------------------------
        # Stage 4: Data Encrypt (optional) - parallel
        # ---------------------------
        if run_encrypt and enc_enabled:
            # Resume: if encrypted outputs exist and audit completed, skip
            try:
                expected = []
                base_name, base_ext = os.path.splitext(base_filename)
                if comp_enabled:
                    comp_split_paths = comp_list_matching("{}*{}".format(base_name, comp_ext))
                    seen = set()
                    deduped_paths = []
                    for p in comp_split_paths:
                        if p not in seen:
                            deduped_paths.append(p)
                            seen.add(p)

                    parts_meta = []
                    for i, p in enumerate(deduped_paths, start=1):
                        rows = int(compute_artifact_data_rows(p, file_cfg, compression_cfg, encryption_cfg) or 0)
                        parts_meta.append({"path": p, "part_idx": i, "data_rows": rows})
                else:
                    split_paths = list_matching("{}*{}".format(base_name, base_ext))
                    seen = set()
                    deduped_paths = []
                    for p in split_paths:
                        if p not in seen:
                            deduped_paths.append(p)
                            seen.add(p)

                    parts_meta = []
                    for i, p in enumerate(deduped_paths, start=1):
                        rows = int(compute_artifact_data_rows(p, file_cfg, compression_cfg, encryption_cfg) or 0)
                        parts_meta.append({"path": p, "part_idx": i, "data_rows": rows})

                encrypt_split_paths = encr_list_matching("{}*{}".format(base_name, 'gpg'))
                encrypt_split_file_name_list = [Path(p).name for p in encrypt_split_paths]


                for p in parts_meta:
                    if p["path"].endswith(".gpg") or p["path"].endswith(".pgp"):
                        expected.append(p["path"])
                    else:
                        expected.append(p["path"] + ".gpg")
                already_encrypted = (bool(encrypt_split_paths)) and ((all(file_exists(p) for p in encrypt_split_paths)))

                encrypt_stage_value = "{} {}".format(STAGE_DATA_ENCRYPT, STAGE_COMPLETED)
                encrypt_completed = recovery_enabled and query_audit_stage_completed_flexible(
                    conn=conn,
                    audit_table=audit_table,
                    workflow_nm=workflow_nm,
                    mapping_nm=mapping_nm,
                    job_nm=job_nm,
                    run_id=run_id,
                    stage_value=encrypt_stage_value,
                    alternate_targetnames=encrypt_split_file_name_list,
                    window_start=audit_start,
                    window_end=audit_end,
                    alternate_ranges=alt_audit_ranges,
                    scope=recovery_scope,
                        success_statuses=success_statuses,
                )

                if encrypt_completed and already_encrypted:
                    log_message("[RESUME] Skipping Data Encrypt (audit completed and encrypted files exist)")
                    new_parts = []
                    for p in parts_meta:
                        if p["path"].endswith(".gpg") or p["path"].endswith(".pgp"):
                            new_path = p["path"]
                        else:
                            new_path = p["path"] + ".gpg"
                        new_parts.append({"path": new_path, "part_idx": p["part_idx"], "data_rows": p.get("data_rows", 0)})
                    parts_meta = new_parts
                else:
                    log_message("[ENCRYPT] start files={} max_workers={}".format([os.path.basename(p["path"]) for p in parts_meta], max_workers))

                    def do_one(it: Dict[str, Any]) -> Dict[str, Any]:
                        part_idx = int(it.get("part_idx") or 1)
                        in_path = it["path"]
                        log_message("[ENCRYPT] file_start part_idx={} file={}".format(part_idx, os.path.basename(in_path)))
                        outp = gpg_encrypt_file(in_path, encryption_cfg, retry_cfg, cfg)
                        log_message("[ENCRYPT] file_done part_idx={} output={}".format(part_idx, os.path.basename(outp)))
                        return {"path": outp, "part_idx": part_idx, "data_rows": it.get("data_rows", 0)}

                    parts_meta = parallel_map_files("ENCRYPT", do_one, parts_meta)

                    # Audit one row per artifact with accurate counts
                    for _pm in parts_meta:
                        _tname = build_local_stage_targetname(os.path.basename(_pm["path"]))
                        _tc = int(_pm.get("data_rows") or 0)

                        append_stage_audit_row(
                            audit_buffer, newrelic_cfg, nr_api_key, nr_api_endpoint, job_nm, mapping_nm, workflow_nm, audit_start, audit_end, run_id,
                            status="SUCCESS", targetname=_tname, stage_label=STAGE_DATA_ENCRYPT,
                            reject_count=0, target_count=_tc
                        )
                    flush("TABLE_TO_S3:Data Encrypt Completed")
            except Exception as exc:
                overall_ok = False
                append_stage_audit_row(
                    audit_buffer, newrelic_cfg, nr_api_key, nr_api_endpoint, job_nm, mapping_nm, workflow_nm, audit_start, audit_end, run_id,
                    status="FAILED", targetname=local_targetname, stage_label=STAGE_DATA_ENCRYPT,
                    reject_count=0, target_count=0, error_msg=str(exc)
                )
                flush("TABLE_TO_S3:Data Encrypt Failed")
                log_message("[ERROR] Data Encrypt failed file={} err={}".format(base_filename, exc))
                if should_stop_on_error():
                    break
                continue
        else:
            if not run_encrypt and enc_enabled:
                log_message("[MANUAL_STAGE] start_stage={} => skipping Data Encrypt".format(manual_stage_norm))

        # Trace: list outputs after encrypt stage (or passthrough if not enabled)
        try:
            log_message("[ENCRYPT_OUTPUT] enc_enabled={} files={}".format(
                bool(enc_enabled), [os.path.basename(p.get("path","")) for p in parts_meta]
            ))
        except Exception:
            pass



        # ---------------------------
        # Stage 5: Upload (deferred)
        # ---------------------------
        if not run_upload:
            log_message("[MANUAL_STAGE] start_stage={} => skipping Data Upload".format(manual_stage_norm))
            continue

        try:
            bn, be = os.path.splitext(base_filename)

            if comp_enabled and enc_enabled:
                final_paths = encr_list_matching("{}*{}".format(bn, ".gpg"))
            elif comp_enabled and not enc_enabled:
                final_paths = comp_list_matching("{}*{}".format(bn, comp_ext))
            else:
                final_paths = list_matching("{}*{}".format(bn, be))

            seen = set()
            deduped_paths = []
            for p in final_paths:
                if p not in seen:
                    seen.add(p)
                    deduped_paths.append(p)

            if not deduped_paths:
                msg = "No local artifacts found to upload for base_filename={} under output_dir={}".format(base_filename, output_dir)
                log_message("[ERROR] {}".format(msg))
                append_stage_audit_row(
                    audit_buffer, newrelic_cfg, nr_api_key, nr_api_endpoint, job_nm, mapping_nm, workflow_nm, audit_start, audit_end, run_id,
                    status="FAILED", targetname=local_targetname, stage_label=STAGE_DATA_UPLOAD,
                    reject_count=0, target_count=0, error_msg=msg
                )
                flush("TABLE_TO_S3:Nothing To Upload")
                overall_ok = False
                if should_stop_on_error():
                    break
                else:
                    continue

            try:
                log_message("[UPLOAD_INPUTS] base_filename={} artifacts={} paths={}".format(
                    base_filename, len(deduped_paths), [os.path.basename(p) for p in deduped_paths]
                ))
            except Exception:
                pass

            # ---------------- FIX (A): per-part tokens for DONE auto-uniquify ----------------
            total_parts = len(deduped_paths)

            for idx, local_path in enumerate(deduped_paths, start=1):
                filename = os.path.basename(local_path)

                task_tokens = dict(tokens)
                task_tokens["total_parts"] = str(total_parts)
                task_tokens["part_idx"] = str(idx)

                # For manual/history runs (load.type="M"), upload into the run-date partition (dt={yyyymmdd})
                if load_type == "M":
                    task_tokens["yyyymmdd"] = run_partition_yyyymmdd

                # Build artifact_map for destination variant selection
                dir_name, file_name = os.path.split(local_path)
                raw_name = file_name.split('.')[0]
                parent = "/".join(dir_name.rstrip("/").split("/")[:-1])

                artifact_map = {
                    "RAW": os.path.join(parent, 'extract', raw_name + be),
                    "COMPRESSED": os.path.join(parent, 'compressed', raw_name + be + comp_ext),
                    "ENCRYPTED": os.path.join(parent, 'encrypted', raw_name + be + '.gpg'),
                    "COMPRESSED_ENCRYPTED": os.path.join(parent, 'encrypted', raw_name + be + comp_ext + '.gpg'),
                }

                upload_tasks.append({
                    "audit_start": audit_start,
                    "audit_end": audit_end,
                    "w_start": w_start,
                    "w_end": w_end,
                    "alt_audit_ranges": alt_audit_ranges,
                    "tokens": task_tokens,              # <- UPDATED
                    "base_filename": base_filename,
                    "local_targetname": local_targetname,
                    "filename": filename,
                    "artifact_map": artifact_map,
                })

        except Exception as exc:
            fail_stage_and_audit(STAGE_DATA_UPLOAD, local_targetname, exc, "TABLE_TO_S3:Upload Prepare Failed", audit_start, audit_end)
            if should_stop_on_error():
                break
            continue

    # -------------------------------------------------------------------------
    # Execute UPLOAD tasks
    # -------------------------------------------------------------------------
    def _task_sort_key(t: Dict[str, Any]):
        return (str(t.get("audit_start") or ""), str(t.get("audit_end") or ""), str(t.get("filename") or ""))

    def _parse_data_action(action: str) -> str:
        # action may contain ";DONE=..."
        if not action:
            return ""
        return (action.split(";")[0] or "").strip()

    def _is_error_action(action: str) -> bool:
        return (action or "").lower().startswith("error:")

    def _audit_from_upload_results(task: Dict[str, Any], results: List[Tuple[str, str, str, str]]):
        """
        FIX (B):
          - Use per-destination uploaded filename from S3 key (basename(_key))
          - Compute per-destination upload target_count from the chosen local artifact for that destination
        """
        nonlocal overall_ok
        audit_start = task["audit_start"]
        audit_end = task["audit_end"]

        for (dest_name, _bucket, _key, action) in results:
            dcfg = None
            for dd in destinations:
                if dd.get("name") == dest_name:
                    dcfg = dd
                    break
            audit_status_on_skip = (dcfg.get("audit_status_on_skip") if dcfg else "SKIPPED") or "SKIPPED"

            uploaded_filename = os.path.basename(_key) if _key else (task.get("filename") or "")
            tgt = build_targetname_for_audit(dest_name, uploaded_filename, audit_targetname_template)

            upload_tc = 0
            count_path = ""
            try:
                if dcfg is not None:
                    chosen_local = _pick_local_artifact_for_destination(dcfg, task.get("artifact_map") or {})
                    chosen_lower = (chosen_local or "").lower()

                    if chosen_lower.endswith((".gpg", ".pgp")):
                        amap = task.get("artifact_map") or {}

                        # COMPRESSED_ENCRYPTED -> COMPRESSED; ENCRYPTED -> RAW
                        if os.path.normpath(chosen_local) == os.path.normpath(amap.get("COMPRESSED_ENCRYPTED", "")):
                            count_path = amap.get("COMPRESSED") or ""
                        elif os.path.normpath(chosen_local) == os.path.normpath(amap.get("ENCRYPTED", "")):
                            count_path = amap.get("RAW") or ""
                        else:
                            pass

                        # Fallback: derive base filename and try common extract location.
                        if not count_path or not os.path.exists(count_path):
                            base_no_enc = normalize_filename_for_counts(os.path.basename(chosen_local))
                            parent_dir = os.path.dirname(os.path.dirname(chosen_local))
                            count_path = find_first_existing_candidate([
                                amap.get("RAW", ""),
                                os.path.join(parent_dir, "extract", base_no_enc),
                                os.path.join(parent_dir, "extract", base_no_enc + "*"),
                            ]) or ""
                    else:
                        # Not encrypted: count directly on the chosen artifact (plain or compressed).
                        count_path = chosen_local

                    if not count_path:
                        raise FrameworkError("count_path could not be resolved for chosen_local={}".format(chosen_local))

                    upload_tc = int(compute_artifact_data_rows(count_path, file_cfg, compression_cfg or {}, encryption_cfg or {}) or 0)
            except Exception as _tc_exc:
                log_message("[WARN] upload count compute failed dest={} file={} err={}".format(dest_name, uploaded_filename, _tc_exc))
                upload_tc = 0

            if _is_error_action(action):
                overall_ok = False
                append_stage_audit_row(
                    audit_buffer, newrelic_cfg, nr_api_key, nr_api_endpoint, job_nm, mapping_nm, workflow_nm, audit_start, audit_end, run_id,
                    status="FAILED", targetname=tgt, stage_label=STAGE_DATA_UPLOAD,
                    reject_count=0, target_count=upload_tc, error_msg=action
                )
            else:
                data_action = _parse_data_action(action)
                status = "SUCCESS" if data_action == "UPLOADED" else audit_status_on_skip
                append_stage_audit_row(
                    audit_buffer, newrelic_cfg, nr_api_key, nr_api_endpoint, job_nm, mapping_nm, workflow_nm, audit_start, audit_end, run_id,
                    status=status, targetname=tgt, stage_label=STAGE_DATA_UPLOAD,
                    reject_count=0, target_count=upload_tc
                )

        flush("TABLE_TO_S3:Upload Audited")

    def _should_skip_dest_upload(task: Dict[str, Any], dest_name: str, filename_for_audit: str) -> bool:
        if not (recovery_enabled and resume_mode == "SKIP_SUCCESS"):
            return False
        tgt = build_targetname_for_audit(dest_name, filename_for_audit, audit_targetname_template)
        return query_audit_has_success_status(
            conn, audit_table, workflow_nm, mapping_nm, job_nm,
            task["audit_start"], task["audit_end"], tgt, recovery_scope, run_id, success_statuses
        )
    # ---------------------------------------------------------------------
    # DONE FILE aggregation sequencing tokens (load-level)
    # ---------------------------------------------------------------------
    run_start_dt = None
    run_end_incl_dt = None
    run_tokens = None
    # Build run-level tokens once (used for aggregate done file naming and S3 key templates).
    # We only need this when aggregate done files are enabled.
    if aggregate_done_enabled and run_tokens is None:
        try:
            if load_type == "M":
                run_start_dt, run_end_incl_dt = parse_manual_start_end(load_cfg)
                
                run_tokens = {
                    "yyyymmdd_start": run_start_dt.strftime("%Y%m%d"),
                    "yyyymmdd_end": run_end_incl_dt.strftime("%Y%m%d"),
                    "yyyymmdd": now_eastern_naive().strftime("%Y%m%d"),
                    "yyyymm": now_eastern_naive().strftime("%Y%m"),
                    "hhmm": "0000",
                }
                
            else:
                # best-effort range for automated loads
                run_start_dt = windows[0][0] if windows else now_eastern_naive()
                run_end_incl_dt = (windows[-1][1] - dt.timedelta(seconds=1)) if windows else now_eastern_naive()

                run_tokens = {
                    "yyyymmdd_start": run_start_dt.strftime("%Y%m%d"),
                    "yyyymmdd_end": run_end_incl_dt.strftime("%Y%m%d"),
                    "yyyymmdd": run_start_dt.strftime("%Y%m%d"),
                    "yyyymm": run_start_dt.strftime("%Y%m"),
                    "hhmm": "0000",
                }
                
        except Exception as _tok_exc:
            # If tokens cannot be built, done file aggregation cannot proceed.
            overall_ok = False
            log_message("[ERROR] DONE_AGG token build failed: {}".format(_tok_exc))
            if should_stop_on_error():
                return 1

    def _build_and_upload_aggregate_done_for_dest(dest: Dict[str, Any]):
        """Build + (optional) upload the aggregate done file for ONE destination.

        This is called *immediately after* that destination's data uploads complete successfully.
        If it fails, we fail the job and prevent subsequent destinations from running.
        """
        try:
            nonlocal overall_ok
            dn = dest.get("name", "unknown")
            if not aggregate_done_enabled:
                return
            if not dest_overall_ok.get(dn, True):
                raise FrameworkError("Destination {} has upload errors; skipping done file".format(dn))

            done_cfg_eff = build_dest_done_cfg(done_cfg, dest)
            if not bool(get_config(done_cfg_eff, "enabled", False)):
                log_message("[DONE_AGG_SKIP] dest={} reason='done_file.enabled=false'".format(dn))
                return

            manifest = done_manifest_by_dest.get(dn, []) or []
            run_tokens["yyyymmdd"] = now_eastern_naive().strftime("%Y%m%d")
            run_tokens["yyyymm"] = now_eastern_naive().strftime("%Y%m")
            done_path = write_aggregate_done_file(done_cfg_eff, run_tokens or {}, manifest)
            done_filename = os.path.basename(done_path)

            upload_cfg = get_config(done_cfg_eff, "upload", {"enabled": False}) or {"enabled": False}
            if bool(get_config(upload_cfg, "enabled", False)):
                done_key_template = require_config(upload_cfg, "key_template", "destinations[].done_file.upload")
                done_exists_behavior = get_config(upload_cfg, "exists_behavior", "overwrite")

                bucket_name, _ = parse_s3_uri(require_config(dest, "s3_path", "target.destinations[]"))
                s3_client = get_client_for_bucket(bucket_name, explicit_profile=dest.get("aws_profile"))
                
                done_upload_tokens = {**(run_tokens or {}), "filename": done_filename}
                done_upload_tokens["yyyymmdd"] = now_eastern_naive().strftime("%Y%m%d")
                done_upload_tokens["yyyymm"] = now_eastern_naive().strftime("%Y%m")
                
                bucket2, key2, done_action = upload_file_to_s3_with_policy(
                    s3_client=s3_client,
                    local_path=done_path,
                    target_s3_path=require_config(dest, "s3_path", "target.destinations[]"),
                    key_template=done_key_template,
                    tokens=done_upload_tokens,
                    exists_behavior=done_exists_behavior,
                    retry_cfg=retry_cfg,
                )
                log_message("[DONE_AGG_UPLOAD] dest={} action={} s3://{}/{}".format(dn, done_action, bucket2, key2))
            else:
                done_action = "CREATED_LOCAL"
                log_message("[DONE_AGG_CREATE] dest={} file={} (upload.disabled)".format(dn, done_filename))

            # Audit a final row for the load-level done file (per destination)
            try:
                total_cnt = sum(int(c or 0) for (_, c) in manifest)
            except Exception:
                total_cnt = 0
            append_stage_audit_row(
                audit_buffer, newrelic_cfg, nr_api_key, nr_api_endpoint, job_nm, mapping_nm, workflow_nm,
                run_start_dt or now_eastern_naive(), run_end_incl_dt or now_eastern_naive(), run_id,
                status="SUCCESS",
                targetname="{}:{}".format(dn, done_filename),
                stage_label=STAGE_DATA_UPLOAD,
                reject_count=0,
                target_count=int(total_cnt),
                error_msg="",
            )
            flush("TABLE_TO_S3:Done File Audited")
        except Exception as done_exc:
            msg = truncate_error_message("Aggregate done file failed: {}: {}".format(done_exc.__class__.__name__, str(done_exc)))
            log_message("[ERROR] {}".format(msg))
            append_stage_audit_row(
                audit_buffer, newrelic_cfg, nr_api_key, nr_api_endpoint, job_nm, mapping_nm, workflow_nm,
                now_eastern_naive(), now_eastern_naive(), run_id,
                status="FAILED", targetname="DONE_AGGREGATE", stage_label=STAGE_DATA_UPLOAD,
                reject_count=0, target_count=0, error_msg=msg
            )

    def _build_and_upload_window_done_for_dest_task(dest: Dict[str, Any],
                                                   task: Dict[str, Any],
                                                   manifest: List[Tuple[str, int]]):
        """Build + (optional) upload a DONE file for ONE destination *and* ONE window(task).

        This is used ONLY for load.type='A' when window-scoped DONE files are desired.
        It runs immediately after that window's data uploads finish for the destination.
        """
        
        try:
            dn = dest.get("name", "unknown")
            if not aggregate_done_enabled:
                return

            done_cfg_eff = build_dest_done_cfg(done_cfg, dest)
            if not bool(get_config(done_cfg_eff, "enabled", False)):
                log_message("[DONE_WIN_SKIP] dest={} reason='done_file.enabled=false'".format(dn))
                return

            # Window-scoped tokens
            tkn = dict(task.get("tokens") or {})
            try:
                ws = task.get("w_start") or task.get("audit_start")
                we = task.get("w_end")
                if ws:
                    tkn["yyyymmdd_start"] = fmt_yyyymmdd(ws)
                if we:
                    tkn["yyyymmdd_end"] = fmt_yyyymmdd(we - timedelta(seconds=1))
            except Exception:
                pass

            done_path = write_aggregate_done_file(done_cfg_eff, tkn, manifest or [])
            done_filename = os.path.basename(done_path)

            upload_cfg = get_config(done_cfg_eff, "upload", {"enabled": False}) or {"enabled": False}
            if bool(get_config(upload_cfg, "enabled", False)):
                done_key_template = require_config(upload_cfg, "key_template", "done_file.upload")
                done_exists_behavior = get_config(upload_cfg, "exists_behavior", "overwrite")

                bucket_name, _ = parse_s3_uri(require_config(dest, "s3_path", "target.destinations[]"))
                s3_client = get_client_for_bucket(bucket_name, explicit_profile=dest.get("aws_profile"))

                bucket2, key2, done_action = upload_file_to_s3_with_policy(
                    s3_client=s3_client,
                    local_path=done_path,
                    target_s3_path=require_config(dest, "s3_path", "target.destinations[]"),
                    key_template=done_key_template,
                    tokens={**tkn, "filename": done_filename},
                    exists_behavior=done_exists_behavior,
                    retry_cfg=retry_cfg,
                )
                log_message("[DONE_WIN_UPLOAD] dest={} action={} s3://{}/{}".format(dn, done_action, bucket2, key2))
            else:
                done_action = "CREATED_LOCAL"
                log_message("[DONE_WIN_CREATE] dest={} file={} (upload.disabled)".format(dn, done_filename))

            # Audit the window-level done file
            try:
                total_cnt = sum(int(c or 0) for (_, c) in (manifest or []))
            except Exception:
                total_cnt = 0

            append_stage_audit_row(
                audit_buffer, newrelic_cfg, nr_api_key, nr_api_endpoint, job_nm, mapping_nm, workflow_nm,
                task.get("audit_start") or now_eastern_naive(),
                task.get("audit_end") or now_eastern_naive(),
                run_id,
                status="SUCCESS",
                targetname="{}:{}".format(dn, done_filename),
                stage_label=STAGE_DATA_UPLOAD,
                reject_count=0,
                target_count=int(total_cnt),
                error_msg="",
            )
            flush("TABLE_TO_S3:Done File Audited")
        except Exception as done_exc:
            msg = truncate_error_message("Aggregate done file failed: {}: {}".format(done_exc.__class__.__name__, str(done_exc)))
            log_message("[ERROR] {}".format(msg))
            append_stage_audit_row(
                audit_buffer, newrelic_cfg, nr_api_key, nr_api_endpoint, job_nm, mapping_nm, workflow_nm,
                now_eastern_naive(), now_eastern_naive(), run_id,
                status="FAILED", targetname="DONE_AGGREGATE", stage_label=STAGE_DATA_UPLOAD,
                reject_count=0, target_count=0, error_msg=msg
            )


    upload_tasks_sorted = sorted(upload_tasks, key=_task_sort_key)

    if upload_tasks_sorted:
        try:
            log_message("[UPLOAD_EXEC] type={} tasks={} destinations={} max_workers={}".format(
                execution_type, len(upload_tasks_sorted), [d.get("name") for d in destinations], max_workers
            ))


            if aggregate_done_enabled and execution_type != "sequence":
                raise ConfigError("done_file aggregation sequencing requires target.execution_type=sequence")

            if execution_type == "sequence":
                # Destination-first: dest1 for ALL tasks, then dest2, etc.
                for dest in destinations:
                    dest_name = require_config(dest, "name", "target.destinations[]")
                    log_message("[UPLOAD_EXEC][SEQUENCE] start destination={}".format(dest_name))

                    for task in upload_tasks_sorted:
                        # For recovery skip, we must use what would be uploaded for THIS destination
                        try:
                            chosen_local = _pick_local_artifact_for_destination(dest, task.get("artifact_map") or {})
                            would_upload_filename = os.path.basename(chosen_local)
                        except Exception:
                            would_upload_filename = task.get("filename") or ""

                        if _should_skip_dest_upload(task, dest_name, would_upload_filename):
                            log_message("[RESUME] Skip upload dest={} file={} (already completed)".format(dest_name, would_upload_filename))
                            continue

                        # For load.type=A we want DONE per window; use a per-task manifest buffer.
                        window_manifest_by_dest = None
                        if aggregate_done_enabled and load_type == "A":
                            window_manifest_by_dest = {dest_name: []}
                        results = upload_artifact_to_destinations(
                            get_client_for_bucket_fn=get_client_for_bucket,
                            destinations=[dest],
                            artifact_map=task["artifact_map"],
                            tokens=task["tokens"],
                            retry_cfg=retry_cfg,
                            max_workers=1,  # strict
                            file_cfg=file_cfg,
                            compression_cfg=compression_cfg,
                            encryption_cfg=encryption_cfg,
                            base_done_cfg=done_cfg,
                            aggregate_done_manifest_by_dest=(
                                window_manifest_by_dest if (aggregate_done_enabled and load_type == "A")
                                else (done_manifest_by_dest if aggregate_done_enabled else None)
                            ),
                        )
                        _audit_from_upload_results(task, results)
                        if aggregate_done_enabled and load_type == "A":
                            # Create/upload ONE done file for this window for this destination.
                            try:
                                acts = [str(_act or "") for (_dn, _b, _k, _act) in (results or [])]
                                if any(a.lower().startswith("error") for a in acts):
                                    raise FrameworkError("Upload failed; not generating window done file")
                                _build_and_upload_window_done_for_dest_task(dest, task, (window_manifest_by_dest or {}).get(dest_name, []))
                            except Exception as _dw_exc:
                                overall_ok = False
                                _log_error("DONE_WIN_FAILED", _dw_exc, dest)
                                if should_stop_on_error():
                                    raise
                        _audit_from_upload_results(task, results)
                    if aggregate_done_enabled:
                        try:
                            # results: List[(dest_name, bucket, key, action)]
                            for (_dn, _b, _k, _act) in results or []:
                                if _dn and str(_act).lower().startswith("error"):
                                    dest_overall_ok[_dn] = False
                        except Exception:
                            pass


                        if not overall_ok and should_stop_on_error():
                            raise FrameworkError("Upload failed and on_error=stop")



                    # After this destination's data uploads are fully complete and successful,
                    # build + upload the aggregate done file for this destination.
                    if aggregate_done_enabled and load_type != "A":
                        _build_and_upload_aggregate_done_for_dest(dest)
                        done_agg_performed_in_sequence = True

                    log_message("[UPLOAD_EXEC][SEQUENCE] completed destination={}".format(dest_name))

            else:
                # Parallel per task across destinations
                for task in upload_tasks_sorted:
                    # Filter out already completed destinations (recovery)
                    active_dests = []
                    for d in destinations:
                        dn = require_config(d, "name", "target.destinations[]")
                        try:
                            chosen_local = _pick_local_artifact_for_destination(d, task.get("artifact_map") or {})
                            would_upload_filename = os.path.basename(chosen_local)
                        except Exception:
                            would_upload_filename = task.get("filename") or ""

                        if _should_skip_dest_upload(task, dn, would_upload_filename):
                            log_message("[RESUME] Skip upload dest={} file={} (already completed)".format(dn, would_upload_filename))
                            continue
                        active_dests.append(d)

                    if not active_dests:
                        continue

                    results = upload_artifact_to_destinations(
                        get_client_for_bucket_fn=get_client_for_bucket,
                        destinations=active_dests,
                        artifact_map=task["artifact_map"],
                        tokens=task["tokens"],
                        retry_cfg=retry_cfg,
                        max_workers=max_workers,
                        file_cfg=file_cfg,
                        compression_cfg=compression_cfg,
                        encryption_cfg=encryption_cfg,
                        base_done_cfg=done_cfg,
                        aggregate_done_manifest_by_dest=done_manifest_by_dest if aggregate_done_enabled else None,
                    )
                    _audit_from_upload_results(task, results)
                    if aggregate_done_enabled:
                        try:
                            # results: List[(dest_name, bucket, key, action)]
                            for (_dn, _b, _k, _act) in results or []:
                                if _dn and str(_act).lower().startswith("error"):
                                    dest_overall_ok[_dn] = False
                        except Exception:
                            pass


                    if not overall_ok and should_stop_on_error():
                        raise FrameworkError("Upload failed and on_error=stop")

        except Exception as exc:
            overall_ok = False
            log_exception("UPLOAD_EXEC", exc, run_id=run_id, execution_type=execution_type)
            if should_stop_on_error():
                log_message("[END] FAILED")
                return 1

    # Watermark update (A-mode) - unchanged
    if overall_ok and (require_config(load_cfg, "type", "load") or "").upper() == "A" and windows:
        watermark_cfg = get_config(table_to_s3cfg, "watermark", {"enabled": True}) or {"enabled": True}
        if bool(get_config(watermark_cfg, "enabled", True)):
            update_control_watermark(
                conn, cfg_table, workflow_nm, mapping_nm, job_nm,
                new_start=windows[-1][0],
                new_end=windows[-1][1],
                run_id=run_id,
                try_update_last_upd=True,
            )
    
    if overall_ok and (require_config(load_cfg, "type", "load") or "").upper() == "M":
        update_control_run_id_only(
        conn, cfg_table, workflow_nm, mapping_nm, job_nm,
        run_id=run_id,
        try_update_last_upd=True,
        )

# Purge - unchanged
    when = (get_config(purge_cfg, "when", "on_success") or "on_success").lower()
    if bool(get_config(purge_cfg, "enabled", False)) and (when == "always" or (when == "on_success" and overall_ok)):
        purge_paths(purge_cfg)

    log_message("[END] {}".format("SUCCESS" if overall_ok else "FAILED"))
    return 0 if overall_ok else 1



def s3_to_s3(conn,
             cfg_row: Dict[str, Any],
             cfg: Dict[str, Any],
             cfg_table: str,
             audit_table: str,
             workflow_nm: str,
             mapping_nm: str,
             job_nm: str,
             run_id: str) -> int:

    # ---- Read S3_TO_S3 config
    s3cfg = require_config(cfg, "s3_to_s3_config", "config")
    load_cfg = require_config(s3cfg, "load", "s3_to_s3_config")
    source_cfg = require_config(s3cfg, "source_s3", "s3_to_s3_config")
    target_cfg = require_config(s3cfg, "target", "s3_to_s3_config")
    runtime_cfg = require_config(s3cfg, "runtime", "s3_to_s3_config")

    # ---- Retry policy (local, because build_retry_policy() has a bad get_config signature for S3_TO_S3) ----
    retry_policy = get_config(s3cfg, "retry_policy", None)
    if isinstance(retry_policy, dict):
        retry_cfg = dict(retry_policy)
        retry_cfg.setdefault("max_retries", 5)
        retry_cfg.setdefault("backoff_base_sec", 2)
        retry_cfg.setdefault("backoff_max_sec", 60)
    else:
        retry_cfg = {
            "max_retries": int(get_config(runtime_cfg, "max_retries", 5) or 5),
            "backoff_base_sec": int(get_config(runtime_cfg, "backoff_base_sec", 2) or 2),
            "backoff_max_sec": int(get_config(runtime_cfg, "backoff_max_sec", 60) or 60),
        }

    # ---- Destinations ----
    destinations = get_config(target_cfg, "destinations", None)
    if not destinations:
        destinations = [{
            "name": "default",
            "s3_path": require_config(target_cfg, "s3_path", "target"),
            "key_template": require_config(target_cfg, "key_template", "target"),
            "exists_behavior": require_config(target_cfg, "exists_behavior", "target"),
            "audit_status_on_skip": require_config(target_cfg, "audit_status_on_skip", "target"),
        }]
    audit_targetname_template = get_config(target_cfg, "audit_targetname_template", "{dest}:{filename}")

    # ---- AWS client routing (read from s3_to_s3_config.aws_creds) ----
    aws_cfg = require_config(s3cfg, "aws_creds", "s3_to_s3_config")
    aws_profiles = load_aws_profiles(require_config(aws_cfg, "file_path", "aws_creds"))
    get_client_for_bucket, _ = build_s3_client_router(aws_profiles)

    # ---- Recovery ----
    recovery_cfg = get_config(runtime_cfg, "recovery", {"enabled": True}) or {"enabled": True}
    recovery_enabled = bool(get_config(recovery_cfg, "enabled", True))
    resume_mode = (get_config(recovery_cfg, "resume_mode", "SKIP_SUCCESS") or "SKIP_SUCCESS").upper()
    recovery_scope = (get_config(recovery_cfg, "scope", "SAME_RUN") or "SAME_RUN").upper()
    success_statuses = get_config(recovery_cfg, "success_statuses", ["SUCCESS", "SKIPPED"]) or ["SUCCESS", "SKIPPED"]

    on_error = (require_config(runtime_cfg, "on_error", "runtime") or "stop").lower()

    # ---- Work dir ----
    work_dir = require_config(runtime_cfg, "work_dir", "runtime")
    download_dir = os.path.join(work_dir, "download")
    ensure_directory(download_dir)

    # ---- Source S3 ----
    source_path = require_config(source_cfg, "s3_path", "source_s3")
    src_bucket, _src_prefix = parse_s3_uri(source_path)
    src_profile = get_config(source_cfg, "aws_profile", None)
    src_s3 = get_client_for_bucket(src_bucket, explicit_profile=src_profile)

    include_globs = get_config(source_cfg, "include_globs", ["*"]) or ["*"]

    # describes how data is organized in SOURCE S3 (folder structure)
    # Example: "dt={yyyymmdd}/{filename}"  (recommended when source_s3.s3_path already contains "custdm/cx")
    source_key_template = get_config(source_cfg, "key_template", None)

    datetime_cfg = get_config(s3cfg, "datetime", {}) or {}
    filename_patterns = get_config(datetime_cfg, "filename_patterns", []) or []

    # ---- DONE file enforcement + DONE globs ----
    done_check_cfg = get_config(s3cfg, "done_file_check", {"enabled": False}) or {"enabled": False}
    done_check_enabled = bool(get_config(done_check_cfg, "enabled", False))
    done_globs = get_config(done_check_cfg, "done_globs", ["*.done"]) or ["*.done"]

    def is_done_filename(name: str) -> bool:
        bn = os.path.basename(name or "")
        for g in done_globs:
            try:
                if fnmatch.fnmatch(bn, g):
                    return True
            except Exception:
                continue
        return bn.lower().endswith(".done")

    # ---- Counting mode ----
    # DONE: parse target_count from .done, apply to DATA artifacts (preferred)
    # WC_L: fallback rowcount from local file
    # S3_BYTES: fallback to object size
    count_mode = (get_config(runtime_cfg, "count_mode", "WC_L") or "WC_L").upper()
    count_excludes_header = bool(get_config(runtime_cfg, "count_excludes_header", True))

    # ---- Minimal filename datetime parser ----
    def parse_datetime_from_filename(filename: str, patterns: List[Dict[str, Any]]) -> Optional[dt.datetime]:
        if not patterns:
            return None
        for p in patterns:
            try:
                rgx = p.get("regex") or ""
                fmt = p.get("format") or ""
                grp = int(p.get("group", 1) or 1)
                if not rgx or not fmt:
                    continue
                m = re.search(rgx, filename)
                if not m:
                    continue
                s = m.group(grp)
                return dt.datetime.strptime(s, fmt)
            except Exception:
                continue
        return None

    def normalize_artifact_filename(name: str) -> str:
        bn = os.path.basename(name or "")
        # strip encryption then compression
        for ex in (".gpg", ".pgp"):
            if bn.lower().endswith(ex):
                bn = bn[: -len(ex)]
        for cx in (".gz", ".bz2", ".zip"):
            if bn.lower().endswith(cx):
                bn = bn[: -len(cx)]
        return bn

    def extract_dt_from_name(name: str) -> Optional[str]:
        base = normalize_artifact_filename(name)
        ts = parse_datetime_from_filename(base, filename_patterns)
        if ts:
            return ts.strftime("%Y%m%d")
        m = re.search(r"(\d{8})", base)
        return m.group(1) if m else None

    def parse_done_file_counts(done_path: str) -> Dict[str, Any]:
        """
        Parse a .done file and extract total target_count.

        Supports common patterns like:
          target_count=12345
          TARGET_COUNT: 12345
          "target_count": 12345
        """
        out = {"total_target_count": None}
        try:
            with open(done_path, "r", encoding="utf-8", errors="ignore") as f:
                txt = f.read()
        except Exception as e:
            log_message("[WARN] done file read failed path={} err={}".format(done_path, e))
            return out

        try:
            m = re.search(r'(?i)\btarget_count\b\s*[:=]\s*(\d+)', txt)
            if m:
                out["total_target_count"] = int(m.group(1))
        except Exception:
            pass
        return out

    def compute_wc_count(local_path: str) -> int:
        try:
            header_present = bool(count_excludes_header)
            return int(data_rows_in_text_file(local_path, header_present=header_present) or 0)
        except Exception as _e:
            log_message("[WARN] WC_L count failed file={} err={}".format(os.path.basename(local_path), _e))
            return 0

    windows = build_windows(load_cfg, cfg_row)
    if not windows:
        log_message("[WINDOWS] count=0 (no work)")
        return 0

    # list all keys once - only needed if no source_key_template
    all_keys: List[str] = []
    if not source_key_template:
        log_message("[S3_SOURCE] listing keys under source_path={} bucket={} profile={} include_globs={}".format(
            source_path, src_bucket, (src_profile or "default"), include_globs
        ))
        all_keys = list_s3_keys_under_prefix(src_s3, source_path)
        log_message("[S3_SOURCE] listed keys count={} bucket={} prefix={}".format(len(all_keys), src_bucket, _src_prefix))
    else:
        log_message("[S3_SOURCE] source_key_template provided; will list per-window prefixes. bucket={} source_path={} template={}".format(
            src_bucket, source_path, source_key_template
        ))

    audit_buffer: List[Dict[str, Any]] = []
    overall_ok = True

    def flush(context: str):
        flush_audit_buffer(conn, audit_table, audit_buffer, context=context)


    for (w_start, w_end, tokens) in windows:
        audit_start, audit_end = build_audit_range(load_cfg, w_start, w_end)

        alt_audit_ranges: List[Tuple[dt.datetime, dt.datetime]] = []
        if (load_cfg.get("granularity", "daily") or "daily").lower() == "daily":
            alt_audit_ranges.append((w_start, w_end))
            alt_audit_ranges.append((w_start, w_start))

        # ---- Match keys for this window ----
        matched_keys: List[str] = []

        if source_key_template:
            # List only within the expected dt=YYYYMMDD folder (fast + explicit mapping)
            try:
                temp_tokens = dict(tokens)
                temp_tokens.setdefault("filename", "DUMMY_FILENAME")
                rendered_key = render_template(source_key_template, temp_tokens)

                expected_prefix = os.path.dirname(rendered_key).rstrip("/") + "/"

                base_bucket, base_prefix = parse_s3_uri(source_path)
                base_prefix = (base_prefix or "").strip("/")
                expected_prefix = expected_prefix.lstrip("/")

                if base_prefix:
                    list_path = "s3://{}/{}".format(base_bucket, base_prefix + "/" + expected_prefix)
                else:
                    list_path = "s3://{}/{}".format(base_bucket, expected_prefix)

                log_message("[S3_SOURCE_WINDOW] bucket={} list_path={} window_start={} window_end={}".format(
                    src_bucket, list_path, w_start, w_end
                ))

                window_keys = list_s3_keys_under_prefix(src_s3, list_path)
                log_message("[S3_SOURCE_WINDOW] listed keys count={} bucket={} prefix={}".format(
                    len(window_keys), src_bucket, expected_prefix
                ))

                for key in window_keys:
                    base = os.path.basename(key)
                    if any(fnmatch.fnmatch(base, g) for g in include_globs):
                        matched_keys.append(key)

            except Exception as _e:
                log_message("[WARN] source key_template listing failed; falling back to full scan. err={}".format(_e))
                matched_keys = []

        if not matched_keys and not source_key_template:
            skipped_by_glob = 0
            skipped_by_dt = 0

            for key in all_keys:
                base = os.path.basename(key)

                if not any(fnmatch.fnmatch(base, g) for g in include_globs):
                    skipped_by_glob += 1
                    continue

                ts = parse_datetime_from_filename(base, filename_patterns)
                if ts is None:
                    skipped_by_dt += 1
                    continue

                if ts >= w_start and ts < w_end:
                    matched_keys.append(key)

            log_message("[S3_MATCH_SCAN] window_start={} window_end={} bucket={} source_prefix={} include_globs={} patterns_count={} total_keys={} matched_keys={} skipped_glob={} skipped_dt_parse={}".format(
                w_start, w_end, src_bucket, _src_prefix, include_globs, len(filename_patterns),
                len(all_keys), len(matched_keys), skipped_by_glob, skipped_by_dt
            ))

        done_keys: List[str] = []
        data_keys: List[str] = []
        for k in matched_keys:
            if is_done_filename(os.path.basename(k)):
                done_keys.append(k)
            else:
                data_keys.append(k)

        # Log matches / no matches with bucket details
        if matched_keys:
            log_message("[S3_MATCH] window_start={} window_end={} bucket={} matched_keys={} data_keys={} done_keys={} done_check_enabled={}".format(
                w_start, w_end, src_bucket, len(matched_keys), len(data_keys), len(done_keys), done_check_enabled
            ))
            for k in matched_keys:
                b = os.path.basename(k)
                log_message("[S3_MATCH_FILE] s3://{}/{} is_done={} dt={}".format(
                    src_bucket, k, is_done_filename(b), extract_dt_from_name(b)
                ))
        else:
            log_message("[S3_MATCH_NONE] No matching objects found for window bucket={} source_path={} window_start={} window_end={}".format(
                src_bucket, source_path, w_start, w_end
            ))

        # Enforce missing patterns behavior based on runtime.on_error
        if done_check_enabled:
            if (not data_keys) or (not done_keys):
                missing = []
                if not data_keys:
                    missing.append("DATA")
                if not done_keys:
                    missing.append("DONE")

                msg = "[S3_MATCH_MISSING] Missing required files for window bucket={} source_path={} window_start={} window_end={} missing={} include_globs={} done_globs={}".format(
                    src_bucket, source_path, w_start, w_end, ",".join(missing), include_globs, done_globs
                )
                log_message(msg)

                if on_error == "stop":
                    overall_ok = False
                    try:
                        window_targetname = build_local_stage_targetname("MISSING__{}__{}".format(tokens.get("yyyymmdd", ""), "_".join(missing)))
                    except Exception:
                        window_targetname = "MISSING"

                    append_stage_audit_row(
                        audit_buffer, job_nm, mapping_nm, workflow_nm, audit_start, audit_end, run_id,
                        status="FAILED",
                        targetname=window_targetname,
                        stage_label=STAGE_SOURCE_CHECK,
                        error_msg=msg
                    )
                    flush("S3_TO_S3:Missing Data/Done (on_error=stop)")
                    break

                # on_error=continue
                continue

        if (not matched_keys) and (not done_check_enabled):
            # If nothing matched at all, honor on_error
            msg = "[S3_MATCH_NONE] No matching objects found for window bucket={} source_path={} window_start={} window_end={} include_globs={} template={}".format(
                src_bucket, source_path, w_start, w_end, include_globs, (source_key_template or "")
            )
            log_message(msg)

            if on_error == "stop":
                overall_ok = False
                try:
                    window_targetname = build_local_stage_targetname("NO_MATCH__{}".format(tokens.get("yyyymmdd", "")))
                except Exception:
                    window_targetname = "NO_MATCH"

                append_stage_audit_row(
                    audit_buffer, job_nm, mapping_nm, workflow_nm, audit_start, audit_end, run_id,
                    status="FAILED",
                    targetname=window_targetname,
                    stage_label=STAGE_SOURCE_CHECK,
                    error_msg=msg
                )
                flush("S3_TO_S3:No Matches Found (on_error=stop)")
                break

            continue


        done_target_count_by_dt: Dict[str, int] = {}

        # Stage ordering:
        #  - Download and parse DONE files first (so DATA uploads can use counts)
        #  - Then process DATA files (download + upload)
        ordered_keys = list(done_keys) + list(data_keys)

        audit_buffer: List[Dict[str, Any]] = audit_buffer  # keep same object

        def flush(context: str):
            flush_audit_buffer(conn, audit_table, audit_buffer, context=context)

        for key in ordered_keys:
            base = os.path.basename(key)
            local_path = os.path.join(download_dir, base)
            local_targetname = build_local_stage_targetname(base)
            is_done = is_done_filename(base)

            # ---------------------------
            # Stage 1: Source Check
            # ---------------------------
            try:
                stage_val = "{} {}".format(STAGE_SOURCE_CHECK, STAGE_COMPLETED)
                source_check_completed = recovery_enabled and query_audit_stage_completed_flexible(
                    conn=conn, audit_table=audit_table,
                    workflow_nm=workflow_nm, mapping_nm=mapping_nm, job_nm=job_nm,
                    stage_value=stage_val, scope=recovery_scope, run_id=run_id,
                    targetname=local_targetname,
                    window_start=audit_start, window_end=audit_end,
                    alternate_ranges=alt_audit_ranges,
                    success_statuses=success_statuses,
                )

                if not source_check_completed:
                    if not s3_object_exists(src_s3, src_bucket, key):
                        raise FrameworkError("Source object disappeared: s3://{}/{}".format(src_bucket, key))

                    append_stage_audit_row(
                        audit_buffer, job_nm, mapping_nm, workflow_nm, audit_start, audit_end, run_id,
                        status="SUCCESS", targetname=local_targetname, stage_label=STAGE_SOURCE_CHECK,
                        reject_count=0, target_count=0
                    )
                    flush("S3_TO_S3:Source Check Completed")

            except Exception as exc:
                overall_ok = False
                append_stage_audit_row(
                    audit_buffer, job_nm, mapping_nm, workflow_nm, audit_start, audit_end, run_id,
                    status="FAILED", targetname=local_targetname, stage_label=STAGE_SOURCE_CHECK,
                    error_msg=str(exc)
                )
                flush("S3_TO_S3:Source Check Failed")
                log_message("[ERROR] Source Check failed bucket={} key={} err={}".format(src_bucket, key, exc))
                if on_error == "stop":
                    break
                continue

            # ---------------------------
            # Stage 2: Data Download (resume if completed + local exists)
            # ---------------------------
            s3_size_bytes = 0
            try:
                try:
                    h = src_s3.head_object(Bucket=src_bucket, Key=key)
                    s3_size_bytes = int(h.get("ContentLength") or 0)
                except Exception:
                    s3_size_bytes = 0

                stage_val = "{} {}".format(STAGE_DATA_DOWNLOAD, STAGE_COMPLETED)
                download_completed = recovery_enabled and query_audit_stage_completed_flexible(
                    conn=conn, audit_table=audit_table,
                    workflow_nm=workflow_nm, mapping_nm=mapping_nm, job_nm=job_nm,
                    stage_value=stage_val, scope=recovery_scope, run_id=run_id,
                    targetname=local_targetname,
                    window_start=audit_start, window_end=audit_end,
                    alternate_ranges=alt_audit_ranges,
                    success_statuses=success_statuses,
                )

                if download_completed and file_exists(local_path):
                    log_message("[RESUME] Skipping Data Download (audit completed and local file exists): {}".format(local_path))
                else:
                    log_message("[DOWNLOAD] start src={} dest_local={}".format("s3://{}/{}".format(src_bucket, key), local_path))
                    download_file_from_s3(src_s3, src_bucket, key, local_path, retry_cfg)
                    log_message("[DOWNLOAD] done  src={} dest_local={}".format("s3://{}/{}".format(src_bucket, key), local_path))

                    append_stage_audit_row(
                        audit_buffer, job_nm, mapping_nm, workflow_nm, audit_start, audit_end, run_id,
                        status="SUCCESS", targetname=local_targetname, stage_label=STAGE_DATA_DOWNLOAD,
                        reject_count=0, target_count=0
                    )
                    flush("S3_TO_S3:Data Download Completed")

                # If DONE file, parse and cache target_count per dt
                if is_done and file_exists(local_path):
                    dc = parse_done_file_counts(local_path)
                    dt_key = extract_dt_from_name(base)
                    if dt_key and (dc.get("total_target_count") is not None):
                        done_target_count_by_dt[dt_key] = int(dc["total_target_count"])
                        log_message("[DONE_COUNT] dt={} done_file={} target_count={}".format(
                            dt_key, base, done_target_count_by_dt[dt_key]
                        ))
                    else:
                        log_message("[DONE_COUNT] done_file={} dt={} target_count=None (not found)".format(
                            base, (dt_key or "NA")
                        ))

            except Exception as exc:
                overall_ok = False
                append_stage_audit_row(
                    audit_buffer, job_nm, mapping_nm, workflow_nm, audit_start, audit_end, run_id,
                    status="FAILED", targetname=local_targetname, stage_label=STAGE_DATA_DOWNLOAD,
                    error_msg=str(exc)
                )
                flush("S3_TO_S3:Data Download Failed")
                log_message("[ERROR] Data Download failed bucket={} key={} err={}".format(src_bucket, key, exc))
                if on_error == "stop":
                    break
                continue


            # ---------------------------
            # Stage 3: Data Upload (pure copy; no split/compress/encrypt)
            # ---------------------------
            try:
                filename = os.path.basename(local_path)

                # Resolve target_count:
                # - DONE file: 0
                # - DATA file: prefer DONE target_count for that dt, fallback to WC_L / S3_BYTES
                artifact_target_count = 0
                if not is_done:
                    dt_key = extract_dt_from_name(filename)
                    if count_mode == "DONE" and dt_key and (dt_key in done_target_count_by_dt):
                        artifact_target_count = int(done_target_count_by_dt[dt_key])
                        log_message("[COUNT] file={} dt={} source=done target_count={}".format(
                            filename, dt_key, artifact_target_count
                        ))
                    else:
                        if count_mode == "S3_BYTES":
                            artifact_target_count = int(s3_size_bytes or 0)
                            log_message("[COUNT] file={} dt={} source=s3_bytes target_count={}".format(
                                filename, (dt_key or "NA"), artifact_target_count
                            ))
                        else:
                            artifact_target_count = compute_wc_count(local_path)
                            if artifact_target_count == 0 and s3_size_bytes:
                                # optional soft fallback to bytes if wc fails
                                artifact_target_count = int(s3_size_bytes or 0)
                                log_message("[COUNT] file={} dt={} source=fallback_s3_bytes target_count={}".format(
                                    filename, (dt_key or "NA"), artifact_target_count
                                ))
                            else:
                                log_message("[COUNT] file={} dt={} source=wc_l target_count={}".format(
                                    filename, (dt_key or "NA"), artifact_target_count
                                ))

                if recovery_enabled and resume_mode == "SKIP_SUCCESS":
                    all_done = True
                    for d in destinations:
                        dest_name = require_config(d, "name", "target.destinations[]")
                        tgt = build_targetname_for_audit(dest_name, filename, audit_targetname_template)
                        if not query_audit_has_success_status(
                            conn, audit_table, workflow_nm, mapping_nm, job_nm,
                            audit_start, audit_end, tgt, recovery_scope, run_id, success_statuses
                        ):
                            all_done = False
                            break
                    if all_done:
                        log_message("[RESUME] Skip upload (already completed on all destinations): {}".format(filename))
                        continue

                for d in destinations:
                    dest_name = require_config(d, "name", "target.destinations[]")
                    dest_s3_path = require_config(d, "s3_path", "target.destinations[]")
                    log_message("[UPLOAD_PLAN] file={} dest_name={} dest_s3_path={}".format(filename, dest_name, dest_s3_path))

                results = upload_artifact_to_destinations(
                    get_client_for_bucket_fn=get_client_for_bucket,
                    destinations=destinations,
                    local_path=local_path,
                    tokens=tokens,
                    retry_cfg=retry_cfg,
                )

                for (dest_name, _bucket, _key, action) in results:
                    dcfg = None
                    for dd in destinations:
                        if dd.get("name") == dest_name:
                            dcfg = dd
                            break

                    audit_status_on_skip = (dcfg.get("audit_status_on_skip") if dcfg else "SKIPPED") or "SKIPPED"
                    status = "SUCCESS" if action == "UPLOADED" else audit_status_on_skip
                    targetname = build_targetname_for_audit(dest_name, filename, audit_targetname_template)

                    log_message("[UPLOAD_RESULT] file={} dest_name={} action={} bucket={} key={} target_count={}".format(
                        filename, dest_name, action, _bucket, _key, artifact_target_count
                    ))

                    append_stage_audit_row(
                        audit_buffer, job_nm, mapping_nm, workflow_nm, audit_start, audit_end, run_id,
                        status=status, targetname=targetname, stage_label=STAGE_DATA_UPLOAD,
                        reject_count=0, target_count=int(artifact_target_count or 0)
                    )

                flush("S3_TO_S3:Data Upload Completed/Skipped")

            except Exception as exc:
                overall_ok = False
                append_stage_audit_row(
                    audit_buffer, job_nm, mapping_nm, workflow_nm, audit_start, audit_end, run_id,
                    status="FAILED", targetname=local_targetname, stage_label=STAGE_DATA_UPLOAD,
                    error_msg=str(exc)
                )
                flush("S3_TO_S3:Data Upload Failed")
                log_message("[ERROR] Data Upload failed src_bucket={} key={} err={}".format(src_bucket, key, exc))
                if on_error == "stop":
                    break
                continue

        if not overall_ok and on_error == "stop":
            break

    # Watermark update (A-mode) - unchanged (kept)
    if overall_ok and (require_config(load_cfg, "type", "load") or "").upper() == "A" and windows:
        watermark_cfg = get_config(s3cfg, "watermark", {"enabled": True}) or {"enabled": True}
        if bool(get_config(watermark_cfg, "enabled", True)):
            update_control_watermark(
                conn, cfg_table, workflow_nm, mapping_nm, job_nm,
                new_start=windows[-1][0],
                new_end=windows[-1][1],
                run_id=run_id,
                try_update_last_upd=True,
            )

    # Purge (kept)
    purge_cfg = get_config(cfg, "purge", {"enabled": False}) or {"enabled": False}
    when = (get_config(purge_cfg, "when", "on_success") or "on_success").lower()
    if bool(get_config(purge_cfg, "enabled", False)) and (when == "always" or (when == "on_success" and overall_ok)):
        purge_paths(purge_cfg)

    log_message("[END] {}".format("SUCCESS" if overall_ok else "FAILED"))
    return 0 if overall_ok else 1



# ============================================================================
# Log filename builder
# ============================================================================
def build_log_filename(runtime_cfg: Dict[str, Any],
                       workflow_nm: str,
                       mapping_nm: str,
                       job_nm: str,
                       run_id: str) -> str:
    template = require_config(runtime_cfg, "log_file_name_template", "runtime")
    log_ts = now_eastern_naive().strftime("%Y%m%d_%H%M%S")
    rendered = render_template(template, {
        "workflow_nm": workflow_nm,
        "mapping_nm": mapping_nm,
        "job_nm": job_nm,
        "run_id": run_id,
        "log_ts": log_ts,
    })
    if ("{log_ts}" not in template) and bool(get_config(runtime_cfg, "append_timestamp_to_log_filename", True)):
        if rendered.lower().endswith(".log"):
            rendered = rendered[:-4] + "__{}.log".format(log_ts)
        else:
            rendered = rendered + "__{}".format(log_ts)
    return rendered


# ============================================================================
# Entrypoint
# ============================================================================
def main() -> int:
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--workflow_nm", required=True)
    parser.add_argument("--mapping_nm", required=True)
    parser.add_argument("--job_nm", required=True)
    parser.add_argument("--run_id", required=False, default="")

    parser.add_argument("--ora_dsn", required=True)
    parser.add_argument("--ora_user", required=True)
    parser.add_argument("--ora_password", required=True)

    parser.add_argument("--cfg_table", required=True)
    parser.add_argument("--audit_table", required=True)
    parser.add_argument("--manual_stage", required=False, default="", help="For load.type=M only: ALL_STAGES|EXTRACT|SPLIT|COMPRESS|ENCRYPT|UPLOAD (start stage)")
    parser.add_argument("--nr_api_key", required=True)
    parser.add_argument("--nr_api_endpoint", required=True)
        
    args = parser.parse_args()

    workflow_nm = args.workflow_nm
    mapping_nm = args.mapping_nm
    job_nm = args.job_nm
    run_id = (args.run_id or "").strip() or uuid.uuid4().hex[:12]
    nr_api_key = args.nr_api_key
    nr_api_endpoint = args.nr_api_endpoint

    conn = create_oracle_connection(args.ora_dsn, args.ora_user, args.ora_password)
    try:
        cfg_row = fetch_config_row(conn, args.cfg_table, workflow_nm, mapping_nm, job_nm)
        cfg_text = coerce_oracle_lob_to_str(require_config(cfg_row, "config_json", "PROCESS_METADATA_CFG"))
        cfg = json.loads(cfg_text)
        mode = (require_config(cfg, "mode", "config") or "").upper()
        if mode == "TABLE_TO_S3":
            table_to_s3cfg = require_config(cfg, "table_to_s3_config", "config")
            runtime_cfg = require_config(table_to_s3cfg, "runtime", "table_to_s3_config")
        else:
            s3cfg = require_config(cfg, "s3_to_s3_config", "config")
            runtime_cfg = require_config(s3cfg, "runtime", "s3_to_s3_config")

        initialize_log_file(
            require_config(runtime_cfg, "log_dir", "runtime"),
            build_log_filename(runtime_cfg, workflow_nm, mapping_nm, job_nm, run_id),
        )
        log_message("[BOOT] workflow={} mapping={} job={} run_id={}".format(workflow_nm, mapping_nm, job_nm, run_id))

        lock_file_path = build_lock_file_path(require_config(runtime_cfg, "lock_dir", "runtime"), workflow_nm, mapping_nm, job_nm)
        acquire_process_lock(lock_file_path)
        log_message("[LOCK] acquired {}".format(lock_file_path))

        try:
            mode = (require_config(cfg, "mode", "config") or "").upper()
            if mode == "TABLE_TO_S3":
                return table_to_s3(conn, nr_api_key, nr_api_endpoint, cfg_row, cfg, args.cfg_table, args.audit_table, workflow_nm, mapping_nm, job_nm, run_id, manual_stage=args.manual_stage)
            if mode == "S3_TO_S3":
                return s3_to_s3(conn, cfg_row, cfg, args.cfg_table, args.audit_table, workflow_nm, mapping_nm, job_nm, run_id)
            raise ConfigError("Unsupported config.mode '{}'. Use TABLE_TO_S3 or S3_TO_S3".format(mode))
        finally:
            release_process_lock(lock_file_path)
            log_message("[LOCK] released {}".format(lock_file_path))
            close_log_file()
    finally:
        try:
            conn.close()
        except Exception:
            pass



if __name__ == "__main__":
    sys.exit(main())