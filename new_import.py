# new_import.py
# last modified date (dd/mm/yy): 27/08/26
# Purpose: Import yard and Intellitrans wagon data, deduplicate, merge, resolve tracks to areas via
# [gvp].[tracklist], assign material GMIDs via [gvp].[materials], and store a timestamped snapshot
# in Azure SQL: [main_data] holds current state, [main_data_history] accumulates every run.
# Transform steps are importable functions shared with backfill_history.py.

# ---------------------------------------------------------------------------------------------------------------------------
# IMPORT LIBRARIES
# ---------------------------------------------------------------------------------------------------------------------------

import re
import uuid
from pathlib import Path

import pandas as pd
import sqlalchemy as dbo
from sqlalchemy.dialects.mssql import DATETIME2
from loguru import logger

from connection import DBConnection
from core import env

# ---------------------------------------------------------------------------------------------------------------------------
# MODULE CONSTANTS, SCHEMA VALIDATION AGAINST INJECTION, SQL COLUMN TYPES
# ---------------------------------------------------------------------------------------------------------------------------

SCHEMA_NAME = env("SCHEMA_NAME", default="gvp")
CSV_TRACK_COL = "CSV_CURRENT TRACK"
STATION_ID = 125

if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", SCHEMA_NAME):
    logger.error(f"Invalid SCHEMA_NAME: {SCHEMA_NAME!r}")
    raise SystemExit(1)

SQL_DTYPES = {
    "EQUIPMENTNUMBER": dbo.NVARCHAR(30),
    "LOADEMPTY": dbo.NVARCHAR(5),
    "LOADSTATUSN": dbo.NVARCHAR(5),
    "TRACKNAME": dbo.NVARCHAR(100),
    "TRACK_RESOLVED": dbo.NVARCHAR(100),
    "TRACK_SOURCE_MISMATCH": dbo.Boolean(),
    "TRACK_ID": dbo.BigInteger(),
    "AREA_ID": dbo.BigInteger(),
    "material_id": dbo.BigInteger(),
    "SNAPSHOT_TS_UTC": DATETIME2(),
    "RUN_ID": dbo.CHAR(32),
}

COLUMNS_TO_DROP = ["BATCH",
                   "BADORDER",
                   "BOLNUMBER",
                   "COMPANYFLEET",
                   "CSV_FUTURE TRACK",
                   "CSV_FUTURE TRACK SPOT",
                   "CSV_GROSS RAIL",
                   "CSV_LOT NUMBER",
                   "CSV_Rail Fleet Admin",
                   "CSV_SEALS",
                   "CSV_SHUNT COMMENTS",
                   "CSV_STATIONID",
                   "CSV_YARD",
                   "CUSTOMERTRIPID",
                   "DEPARTUREDATE",
                   "DEPARTMENTNAME",
                   "DEPARTMENTREP",
                   "GMID_FLEET",
                   "GMID_SUBFLEET",
                   "GRAVITY",
                   "LOADERINITIALS",
                   "LOADNUMBER",
                   "LOTNUMBER",
                   "MEDIC",
                   "OUTAGE",
                   "PHONE",
                   "PONUMBER",
                   "SAMPLETESTNUMBER",
                   "SEALNO1",
                   "SEALNO2",
                   "SEALNO3",
                   "SEALNO4",
                   "SEALNO5",
                   "SOURCE",
                   "SPOTID",
                   "STATIC_FLEET",
                   "STATIC_SUBFLEET",
                   "TANKNUMBER",
                   "TEMPERATURE",
                   "TICKETNUMBER"
                   ]

# ---------------------------------------------------------------------------------------------------------------------------
# DATABASE HELPERS
# ---------------------------------------------------------------------------------------------------------------------------

def get_engine():
    try:
        dbc = DBConnection("DEV")
        return dbo.create_engine("mssql+pyodbc://", creator=lambda: dbc.get_conn(), pool_size=10, max_overflow=0)
    except Exception as e:
        logger.error(f"Error creating SQLAlchemy engine: {e}")
        raise SystemExit(1)


def read_tracklist(engine):
    tracklist_df = pd.read_sql(f"SELECT trackid, tracknr, trackname, area FROM [{SCHEMA_NAME}].[tracklist]", engine)
    tracklist_df["trackname_norm"] = tracklist_df["trackname"].astype("string").str.strip().str.upper()
    duplicate_tracks = tracklist_df[tracklist_df.duplicated("trackname_norm", keep=False)]
    if not duplicate_tracks.empty:
        logger.error(f"Duplicate tracknames in [{SCHEMA_NAME}].[tracklist], fix before import can run:\n{duplicate_tracks.to_string(index=False)}")
        raise SystemExit(1)
    return tracklist_df


def read_materials(engine):
    material_df = pd.read_sql(f"SELECT material_id, match_pattern FROM [{SCHEMA_NAME}].[materials]", engine)
    if material_df.empty:
        logger.error(f"[{SCHEMA_NAME}].[materials] is empty, run compliance_setup.sql first")
        raise SystemExit(1)
    return material_df

# ---------------------------------------------------------------------------------------------------------------------------
# TRANSFORM STEP, LOAD YARD DATA, KEEP MOST RECENT RECORD PER WAGON, FILTER TO STATIONID
# ---------------------------------------------------------------------------------------------------------------------------

def load_yard_data(yard_path):
    df = pd.read_csv(yard_path, sep="|")
    df.columns = df.columns.str.strip()
    logger.info(f"Loaded {len(df)} rows from {yard_path.name}")

    df["MODIFYDATE"] = pd.to_datetime(df["MODIFYDATE"], format="%Y-%m-%d", errors="coerce")
    df["CREATEDATE"] = pd.to_datetime(df["CREATEDATE"], format="%Y-%m-%d", errors="coerce")
    df["PRIORITY_DATE"] = df["MODIFYDATE"].fillna(df["CREATEDATE"])

    df_sorted = df.sort_values(["EQUIPMENTNUMBER", "PRIORITY_DATE"], ascending=[True, False], na_position="last")
    df = df_sorted.drop_duplicates(subset=["EQUIPMENTNUMBER"], keep="first").drop("PRIORITY_DATE", axis=1)
    logger.info(f"Deduplicated to {len(df)} unique wagons")

    rows_before_filter = len(df)
    df = df[df["STATIONID"] == STATION_ID].reset_index(drop=True)
    logger.info(f"STATIONID {STATION_ID} filter: {rows_before_filter} -> {len(df)} rows")
    return df

# ---------------------------------------------------------------------------------------------------------------------------
# TRANSFORM STEP, MERGE WITH INTELLITRANS EXPORT, PREFIX CSV COLUMNS, OUTER JOIN ON EQUIPMENTNUMBER
# ---------------------------------------------------------------------------------------------------------------------------

def merge_intellitrans(df, csv_path):
    if csv_path is None:
        logger.warning("No Intellitrans file provided, continuing without merge")
        return df
    try:
        csv_df = pd.read_csv(csv_path, encoding="cp1252")
        csv_df.columns = csv_df.columns.str.strip()
        logger.info(f"Loaded {len(csv_df)} rows from {csv_path.name}")

        if "EQUIPMENT" not in csv_df.columns:
            raise ValueError(f"EQUIPMENT column missing, available columns: {list(csv_df.columns)}")

        csv_df = csv_df.rename(columns={col: f"CSV_{col}" for col in csv_df.columns if col != "EQUIPMENT"})
        csv_df = csv_df.rename(columns={"EQUIPMENT": "EQUIPMENTNUMBER"})

        csv_df["EQUIPMENTNUMBER"] = csv_df["EQUIPMENTNUMBER"].astype("string").str.strip()
        df["EQUIPMENTNUMBER"] = df["EQUIPMENTNUMBER"].astype("string").str.strip()

        common_equipment = set(df["EQUIPMENTNUMBER"]) & set(csv_df["EQUIPMENTNUMBER"])
        if not common_equipment:
            logger.warning("No common EQUIPMENTNUMBER values between yard data and Intellitrans")

        df = df.merge(csv_df, on="EQUIPMENTNUMBER", how="outer")
        logger.info(f"Merged dataframe: {len(df)} rows, {len(df.columns)} columns")
    except FileNotFoundError:
        logger.warning(f"{csv_path} not found, continuing without merge")
    except Exception as e:
        logger.error(f"Error merging Intellitrans file: {e}")
    return df

# ---------------------------------------------------------------------------------------------------------------------------
# TRANSFORM STEP, DROP COLUMNS NOT NEEDED FOR COMPLIANCE REPORTING
# ---------------------------------------------------------------------------------------------------------------------------

def drop_unused_columns(df):
    return df.drop(COLUMNS_TO_DROP, axis=1, errors="ignore")

# ---------------------------------------------------------------------------------------------------------------------------
# TRANSFORM STEP, CREATE LOADSTATUSN, FLIP LOAD STATUS FOR WAGONS WITH CSV_STATUS READY FOR DISPATCH
# ---------------------------------------------------------------------------------------------------------------------------

def apply_load_status(df):
    if "LOADEMPTY" in df.columns:
        df["LOADSTATUSN"] = df["LOADEMPTY"].copy()
    else:
        logger.warning("LOADEMPTY column not found, cannot create LOADSTATUSN")

    if "CSV_STATUS" in df.columns:
        df["CSV_STATUS"] = df["CSV_STATUS"].astype("string").str.replace(r"^\d+\s+", "", regex=True)
        if "LOADEMPTY" in df.columns:
            condition_e_to_l = (df["LOADEMPTY"] == "E") & (df["CSV_STATUS"] == "Ready for Dispatch")
            condition_l_to_e = (df["LOADEMPTY"] == "L") & (df["CSV_STATUS"] == "Ready for Dispatch")
            df.loc[condition_e_to_l, "LOADSTATUSN"] = "L"
            df.loc[condition_l_to_e, "LOADSTATUSN"] = "E"
            logger.info(f"LOADSTATUSN flipped for {int(condition_e_to_l.sum())} E->L and {int(condition_l_to_e.sum())} L->E wagons")
    else:
        logger.info("CSV_STATUS column not found, skipping status processing")

    if "LOADSTATUSN" in df.columns:
        logger.info(f"LOADSTATUSN value counts:\n{df['LOADSTATUSN'].value_counts().to_string()}")
    return df

# ---------------------------------------------------------------------------------------------------------------------------
# TRANSFORM STEP, MATCH TRACKS TO AREAS, CSV_CURRENT TRACK WINS OVER TRACKNAME
# ---------------------------------------------------------------------------------------------------------------------------

def match_tracks(df, tracklist_df):
    if CSV_TRACK_COL in df.columns:
        csv_track = df[CSV_TRACK_COL].astype("string").str.strip().str.upper().replace("", pd.NA)
    else:
        logger.warning(f"Column {CSV_TRACK_COL} not found, matching on TRACKNAME only")
        csv_track = pd.Series(pd.NA, index=df.index, dtype="string")

    yard_track = df["TRACKNAME"].astype("string").str.strip().str.upper().replace("", pd.NA)

    df["TRACK_RESOLVED"] = csv_track.fillna(yard_track)
    df["TRACK_SOURCE_MISMATCH"] = csv_track.notna() & yard_track.notna() & (csv_track != yard_track)

    mismatch_count = int(df["TRACK_SOURCE_MISMATCH"].sum())
    if mismatch_count > 0:
        logger.warning(f"{mismatch_count} wagons where {CSV_TRACK_COL} and TRACKNAME disagree, {CSV_TRACK_COL} used:")
        logger.warning(f"\n{df.loc[df['TRACK_SOURCE_MISMATCH'], ['EQUIPMENTNUMBER', 'TRACKNAME', CSV_TRACK_COL]].head(20).to_string(index=False)}")

    df = df.merge(
        tracklist_df[["trackname_norm", "trackid", "area"]].rename(columns={"trackid": "TRACK_ID", "area": "AREA_ID"}),
        left_on="TRACK_RESOLVED",
        right_on="trackname_norm",
        how="left"
    ).drop("trackname_norm", axis=1)

    df["TRACK_ID"] = df["TRACK_ID"].astype("Int64")
    df["AREA_ID"] = df["AREA_ID"].astype("Int64")

    no_track = df["TRACK_RESOLVED"].isna()
    unmatched = df["TRACK_RESOLVED"].notna() & df["AREA_ID"].isna()
    logger.info(f"Track matching: {len(df)} wagons, {int((~no_track & ~unmatched).sum())} matched to an area")
    if no_track.any():
        logger.warning(f"{int(no_track.sum())} wagons have no track in either source, they will count toward NO area")
    if unmatched.any():
        logger.warning(f"{int(unmatched.sum())} wagons stand on tracks missing from [{SCHEMA_NAME}].[tracklist]:")
        logger.warning(f"\n{df.loc[unmatched, 'TRACK_RESOLVED'].value_counts().to_string()}")
    return df

# ---------------------------------------------------------------------------------------------------------------------------
# TRANSFORM STEP, ASSIGN MATERIAL GMIDS, LONGEST PATTERN FIRST SO HEAVY C4 IS NOT OVERWRITTEN BY C4
# ---------------------------------------------------------------------------------------------------------------------------

def assign_materials(df, material_df):
    df["material_id"] = pd.Series(pd.NA, index=df.index, dtype="Int64")

    if "CSV_MATERIAL DESCRIPTION" in df.columns:
        for _, row in material_df.sort_values("match_pattern", key=lambda s: s.str.len(), ascending=False).iterrows():
            mask = df["material_id"].isna() & df["CSV_MATERIAL DESCRIPTION"].str.contains(row["match_pattern"], case=False, na=False, regex=False)
            df.loc[mask, "material_id"] = row["material_id"]
        logger.info(f"material_id assigned for {int(df['material_id'].notna().sum())} of {len(df)} wagons")
        unmatched_materials = df.loc[df["material_id"].isna() & df["CSV_MATERIAL DESCRIPTION"].notna(), "CSV_MATERIAL DESCRIPTION"]
        if not unmatched_materials.empty:
            logger.warning(f"Material descriptions not matched to any material_id, they will count toward NO limit:\n{unmatched_materials.value_counts().to_string()}")
    else:
        logger.warning("CSV_MATERIAL DESCRIPTION column not found, material_id left empty")
    return df

# ---------------------------------------------------------------------------------------------------------------------------
# FULL TRANSFORM PIPELINE FOR ONE SNAPSHOT, SHARED BY THE LIVE RUN AND THE BACKFILL
# ---------------------------------------------------------------------------------------------------------------------------

def process_snapshot(yard_path, csv_path, tracklist_df, material_df):
    df = load_yard_data(yard_path)
    df = merge_intellitrans(df, csv_path)
    df = drop_unused_columns(df)
    df = apply_load_status(df)
    df = match_tracks(df, tracklist_df)
    df = assign_materials(df, material_df)
    return df

# ---------------------------------------------------------------------------------------------------------------------------
# MAIN, 15 MINUTE SCHEDULED RUN, WRITES CURRENT STATE AND APPENDS HISTORY
# ---------------------------------------------------------------------------------------------------------------------------

def main():
    logger.add("new_import.log", rotation="10 MB", retention=10, backtrace=False, diagnose=False)

    yard_path = Path(env("TRAIN_CSV_DATA_LOCATION_1", required=True))
    csv_path = Path(env("TRAIN_CSV_DATA_LOCATION_2", required=True))
    excel_path = Path(env("TRAIN_OUTPUT_DATA_LOCATION_1", required=True))

    run_id = uuid.uuid4().hex
    snapshot_ts_utc = pd.Timestamp.now(tz="UTC").tz_localize(None)
    logger.info(f"Run {run_id} started, snapshot timestamp {snapshot_ts_utc} UTC")

    engine = get_engine()
    tracklist_df = read_tracklist(engine)
    material_df = read_materials(engine)

    try:
        df_final = process_snapshot(yard_path, csv_path, tracklist_df, material_df)
    except FileNotFoundError as e:
        logger.error(f"Input file not found: {e}")
        raise SystemExit(1)

    df_final["SNAPSHOT_TS_UTC"] = snapshot_ts_utc
    df_final["RUN_ID"] = run_id

    try:
        df_final.to_excel(excel_path, index=False, engine="openpyxl")
        logger.info(f"Exported {len(df_final)} rows and {len(df_final.columns)} columns to {excel_path.name}")
    except Exception as e:
        logger.error(f"Error exporting to Excel: {e}")

    try:
        df_final.to_sql(name="main_data", schema=SCHEMA_NAME, con=engine, if_exists="replace", index=False, dtype=SQL_DTYPES)
        df_final.to_sql(name="main_data_history", schema=SCHEMA_NAME, con=engine, if_exists="append", index=False, dtype=SQL_DTYPES)
        logger.info(f"Exported {len(df_final)} rows to [{SCHEMA_NAME}].[main_data] and appended to [{SCHEMA_NAME}].[main_data_history]")
    except Exception as e:
        logger.error(f"Error exporting to SQL: {e}")
        raise SystemExit(1)

    logger.info(f"Run {run_id} finished")


if __name__ == "__main__":
    main()
