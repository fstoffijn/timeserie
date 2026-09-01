# backfill_history.py
# last modified date (dd/mm/yy): 27/08/26
# Purpose: One-time backfill of [gvp].[main_data_history] from a folder of daily historic yard and
# Intellitrans files. Pairs files by the date in their filename, skips dates already present in
# history, reuses the exact transform pipeline from new_import.py, and appends to history only,
# never touching the current-state table.

# ---------------------------------------------------------------------------------------------------------------------------
# IMPORT LIBRARIES
# ---------------------------------------------------------------------------------------------------------------------------

import re
import uuid
from pathlib import Path

import pandas as pd
import sqlalchemy as dbo
from loguru import logger

from core import env
from new_import import (SCHEMA_NAME, SQL_DTYPES, get_engine, read_tracklist, read_materials, process_snapshot)

# ---------------------------------------------------------------------------------------------------------------------------
# CONFIGURATION, ADJUST DATE_REGEX AND GLOBS TO MATCH THE ACTUAL HISTORIC FILENAMES
# BACKFILL_SNAPSHOT_TIME IS THE UTC TIME OF DAY THE DAILY EXPORTS WERE TAKEN
# ---------------------------------------------------------------------------------------------------------------------------

BACKFILL_FOLDER = Path(env("BACKFILL_FOLDER", required=True))
BACKFILL_SNAPSHOT_TIME = env("BACKFILL_SNAPSHOT_TIME", default="06:00")
YARD_GLOB = env("BACKFILL_YARD_GLOB", default="*.txt")
CSV_GLOB = env("BACKFILL_CSV_GLOB", default="*.csv")
DATE_REGEX = re.compile(r"(\d{4}-\d{2}-\d{2})")

# ---------------------------------------------------------------------------------------------------------------------------
# COLLECT DAILY FILE PAIRS FROM THE BACKFILL FOLDER
# ---------------------------------------------------------------------------------------------------------------------------

def extract_date(path):
    match = DATE_REGEX.search(path.name)
    return match.group(1) if match else None


def collect_pairs():
    yard_files = {}
    csv_files = {}
    for path in sorted(BACKFILL_FOLDER.glob(YARD_GLOB)):
        date_str = extract_date(path)
        if date_str is None:
            logger.warning(f"No date found in filename, skipping: {path.name}")
        else:
            yard_files[date_str] = path
    for path in sorted(BACKFILL_FOLDER.glob(CSV_GLOB)):
        date_str = extract_date(path)
        if date_str is None:
            logger.warning(f"No date found in filename, skipping: {path.name}")
        else:
            csv_files[date_str] = path
    all_dates = sorted(set(yard_files) | set(csv_files))
    return [(date_str, yard_files.get(date_str), csv_files.get(date_str)) for date_str in all_dates]

# ---------------------------------------------------------------------------------------------------------------------------
# HISTORY TABLE HELPERS, SKIP DATES ALREADY LOADED AND ALIGN COLUMNS AGAINST SCHEMA DRIFT
# ---------------------------------------------------------------------------------------------------------------------------

def existing_snapshot_dates(engine):
    if not dbo.inspect(engine).has_table("main_data_history", schema=SCHEMA_NAME):
        logger.info(f"[{SCHEMA_NAME}].[main_data_history] does not exist yet, it will be created on first append")
        return set()
    existing = pd.read_sql(f"SELECT DISTINCT CAST(SNAPSHOT_TS_UTC AS date) AS snapshot_date FROM [{SCHEMA_NAME}].[main_data_history]", engine)
    return set(existing["snapshot_date"].astype(str))


def align_columns(df, engine):
    inspector = dbo.inspect(engine)
    if not inspector.has_table("main_data_history", schema=SCHEMA_NAME):
        return df
    table_columns = [col["name"] for col in inspector.get_columns("main_data_history", schema=SCHEMA_NAME)]
    extra = [col for col in df.columns if col not in table_columns]
    if extra:
        logger.warning(f"Columns in historic file not present in history table, dropped: {extra}")
        df = df.drop(extra, axis=1)
    missing = [col for col in table_columns if col not in df.columns]
    if missing:
        logger.warning(f"History table columns missing in historic file, stored as NULL: {missing}")
    return df

# ---------------------------------------------------------------------------------------------------------------------------
# MAIN, PROCESS EVERY DAILY PAIR, ONE FAILED DAY DOES NOT STOP THE REST
# ---------------------------------------------------------------------------------------------------------------------------

def main():
    logger.add("backfill_history.log", rotation="10 MB", retention=10, backtrace=False, diagnose=False)

    engine = get_engine()
    tracklist_df = read_tracklist(engine)
    material_df = read_materials(engine)
    existing_dates = existing_snapshot_dates(engine)

    pairs = collect_pairs()
    if not pairs:
        logger.error(f"No dated files found in {BACKFILL_FOLDER}")
        raise SystemExit(1)
    logger.info(f"Found {len(pairs)} dated file sets in {BACKFILL_FOLDER}, {len(existing_dates)} dates already in history")

    loaded = skipped = failed = 0
    for date_str, yard_path, csv_path in pairs:
        if date_str in existing_dates:
            logger.info(f"{date_str}: snapshot already in history, skipped")
            skipped += 1
            continue
        if yard_path is None:
            logger.warning(f"{date_str}: yard file missing, only {csv_path.name} found, skipped")
            failed += 1
            continue
        if csv_path is None:
            logger.warning(f"{date_str}: no Intellitrans file, processing yard data alone")
        try:
            df = process_snapshot(yard_path, csv_path, tracklist_df, material_df)
            df["SNAPSHOT_TS_UTC"] = pd.Timestamp(f"{date_str} {BACKFILL_SNAPSHOT_TIME}")
            df["RUN_ID"] = uuid.uuid4().hex
            df = align_columns(df, engine)
            df.to_sql(name="main_data_history", schema=SCHEMA_NAME, con=engine, if_exists="append", index=False, dtype=SQL_DTYPES)
            logger.info(f"{date_str}: appended {len(df)} rows to [{SCHEMA_NAME}].[main_data_history]")
            loaded += 1
        except Exception as e:
            logger.error(f"{date_str}: failed, {e}")
            failed += 1

    logger.info(f"Backfill finished: {loaded} days loaded, {skipped} skipped, {failed} failed")
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
