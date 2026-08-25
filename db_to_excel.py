# db_to_excel.py
# Last modified date (dd/mm/yy): 25/08/26
# Purpose: Export merged tank tag data from Azure SQL to per-file Excel workbooks for Power BI,
#          including safe evaluation of tag calculation expressions and fallback calculation of
#          Inhoud (MT) from percentage level tags (UnitOfMeasure = pct) combined with tank
#          master data (maxinhoud, or maxvolume x density once density is available).

# --------------------------------------------------------------------------------------------------------------
# LOAD LIBRARIES
# --------------------------------------------------------------------------------------------------------------

import os
import ast
import math
import operator
import re
import struct
import logging
from pathlib import Path

import pandas as pd
import pyodbc as db
import sqlalchemy as dbo
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv

# --------------------------------------------------------------------------------------------------------------
# CONFIGURATION AND ENVIRONMENT SETUP
# --------------------------------------------------------------------------------------------------------------

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ENV_VERSION = os.getenv('ENV_VERSION', 'DEV')

DESC_INHOUD_DEFAULT = 'Inhoud (MT)'
DESC_INHOUD_ALIASES = {'inhoud (mt)', 'actuele inhoud (mt)'}
DESC_NIVEAU_ALIASES = {'niveau (%)', 'niveau (pct)', 'niveau(%)'}
UOM_PCT_ALIASES = {'pct', '%'}
TANKSHAPE_VERTICAL_ALIASES = {'', 'nan', 'none', 'vertical', 'verticaal'}

SOURCE_MEASURED = 'measured'
SOURCE_MAXINHOUD = 'calculated_maxinhoud'
SOURCE_DENSITY = 'calculated_density'
SOURCE_SKIPPED_SHAPE = 'skipped_tankshape'
SOURCE_MISSING_NIVEAU = 'missing_niveau'
SOURCE_MISSING_MASTERDATA = 'missing_masterdata'

NIVEAU_MIN_PCT = 0.0
NIVEAU_MAX_PCT = 105.0
DENSITY_MIN_T_M3 = 0.05
DENSITY_MAX_T_M3 = 5.0

# --------------------------------------------------------------------------------------------------------------
# DATABASE CONNECTION CLASS
# --------------------------------------------------------------------------------------------------------------


class DBConnection:
    def __init__(self, env_version: str = ENV_VERSION):
        self.env_version = env_version.upper()
        self.connection_params = self._get_connection_params()
        self._conn = None

    def _get_connection_params(self) -> dict:
        env_suffix = self.env_version
        required_vars = [
            f"AZURE_SQL_CONNECTIONSTRING_{env_suffix}",
            f'SQL_DRIVER_{env_suffix}',
            f'SQL_SERVER_{env_suffix}',
            f'SQL_DATABASE_{env_suffix}'
        ]

        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")

        return {
            'connection_string': os.getenv(f"AZURE_SQL_CONNECTIONSTRING_{env_suffix}"),
            'driver': os.getenv(f'SQL_DRIVER_{env_suffix}'),
            'server': os.getenv(f'SQL_SERVER_{env_suffix}'),
            'database': os.getenv(f'SQL_DATABASE_{env_suffix}')
        }

    def get_connection(self):
        try:
            credential = DefaultAzureCredential(exclude_interactive_browser_credential=False)
            token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
            token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
            SQL_COPT_SS_ACCESS_TOKEN = 1256

            self._conn = db.connect(
                self.connection_params['connection_string'],
                attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct}
            )
            return self._conn
        except Exception as e:
            logger.error(f"Failed to establish database connection: {e}")
            raise

    def get_engine(self):
        return dbo.create_engine(
            'mssql+pyodbc://',
            creator=lambda: self.get_connection(),
            pool_size=5,
            max_overflow=2,
            pool_timeout=30,
            pool_recycle=3600
        )

# --------------------------------------------------------------------------------------------------------------
# SAFE CALCULATION EVALUATION
# --------------------------------------------------------------------------------------------------------------

ALLOWED_OPERATORS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.USub: operator.neg,
    ast.UAdd: operator.pos,
}

VALID_CALC_PATTERN = re.compile(r'^[0-9+\-*/(). ]+$')


def safe_eval_expression(expression: str, value: float) -> float:
    tree = ast.parse(expression, mode='eval')

    def evaluate(node):
        if isinstance(node, ast.Expression):
            return evaluate(node.body)
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            return float(node.value)
        if isinstance(node, ast.Name) and node.id == 'v':
            return float(value)
        if isinstance(node, ast.BinOp) and type(node.op) in ALLOWED_OPERATORS:
            return ALLOWED_OPERATORS[type(node.op)](evaluate(node.left), evaluate(node.right))
        if isinstance(node, ast.UnaryOp) and type(node.op) in ALLOWED_OPERATORS:
            return ALLOWED_OPERATORS[type(node.op)](evaluate(node.operand))
        raise ValueError("Unsupported element in calculation expression")

    return evaluate(tree)


def apply_calculation(latest_value, calculation_text):
    try:
        value = pd.to_numeric(latest_value, errors='coerce')
        if pd.isna(value) or pd.isna(calculation_text) or str(calculation_text).strip() == '':
            return None

        calc_str = str(calculation_text).strip().replace(',', '.')
        calc_str = calc_str.replace('***', '*').replace('**', '*')

        if not VALID_CALC_PATTERN.match(calc_str):
            logger.warning(f"Rejected calculation '{calculation_text}': contains invalid characters")
            return None

        if calc_str.startswith('/') or calc_str.startswith('*'):
            expression = f"v{calc_str}"
        else:
            expression = f"v*({calc_str})"

        result = safe_eval_expression(expression, float(value))
        return result if math.isfinite(result) else None

    except (ValueError, ZeroDivisionError, SyntaxError) as e:
        logger.warning(f"Error applying calculation '{calculation_text}' to value '{latest_value}': {e}")
        return None

# --------------------------------------------------------------------------------------------------------------
# NORMALIZATION HELPERS FOR DESC, UOM, TANKSHAPE AND TANK KEY MATCHING
# --------------------------------------------------------------------------------------------------------------


def normalize_text(value) -> str:
    if pd.isna(value):
        return ''
    return str(value).strip().lower()


def desc_mask(series: pd.Series, aliases: set) -> pd.Series:
    return series.map(normalize_text).isin(aliases)


def tank_key(messerver, tank) -> tuple:
    return (normalize_text(messerver), normalize_text(tank))


def is_vertical(tankshape) -> bool:
    return normalize_text(tankshape) in TANKSHAPE_VERTICAL_ALIASES


def coalesce_numeric(primary, secondary):
    primary = pd.to_numeric(primary, errors='coerce')
    if pd.notna(primary):
        return primary
    return pd.to_numeric(secondary, errors='coerce')

# --------------------------------------------------------------------------------------------------------------
# LOAD DATA FROM SQL
# --------------------------------------------------------------------------------------------------------------


def load_data_from_sql():
    logger.info("Establishing database connection...")
    db_conn = DBConnection()
    engine = db_conn.get_engine()

    try:
        mestags_query = """
        SELECT [tag], [messerver], [tank], [compression], [Desc], [TankCheck],
               [IP_DESCRIPTION], [maxvolume], [maxinhoud], [coordinates],
               [staticproduct], [tenant], [plant], [area], [contact],
               [staticdesc], [gevicode], [comment], [filename], [calculation],
               [tankshape], [density]
        FROM [mes].[mestags]
        """

        timeseriedata_query = """
        SELECT [TagName], [MESServer], [LatestTs], [LatestValue], [UnitOfMeasure],
               [RecordType], [IP_STEPPED], [TagDescription]
        FROM [mes].[timeseriedata]
        """

        logger.info("Loading mestags data...")
        mestags_df = pd.read_sql_query(mestags_query, engine)

        logger.info("Loading timeseriedata...")
        timeseriedata_df = pd.read_sql_query(timeseriedata_query, engine)

        logger.info(f"Loaded {len(mestags_df)} records from mestags")
        logger.info(f"Loaded {len(timeseriedata_df)} records from timeseriedata")

        return mestags_df, timeseriedata_df

    except Exception as e:
        logger.error(f"Error loading data from SQL: {e}")
        raise
    finally:
        engine.dispose()

# --------------------------------------------------------------------------------------------------------------
# NIVEAU (PCT) LOOKUP BASED ON UNIT OF MEASURE
# --------------------------------------------------------------------------------------------------------------


def build_niveau_lookup(merged_df: pd.DataFrame) -> dict:
    pct_mask = (merged_df['UnitOfMeasure'].map(normalize_text).isin(UOM_PCT_ALIASES) &
                pd.to_numeric(merged_df['LatestValue'], errors='coerce').notna())
    pct_rows = merged_df[pct_mask].copy()
    pct_rows['LatestValue'] = pd.to_numeric(pct_rows['LatestValue'], errors='coerce')
    pct_rows['_is_niveau_desc'] = desc_mask(pct_rows['Desc'], DESC_NIVEAU_ALIASES)
    pct_rows = pct_rows.sort_values(['_is_niveau_desc', 'LatestTs'])

    lookup = {}
    duplicates = set()
    for idx, row in pct_rows.iterrows():
        key = tank_key(row['messerver'], row['tank'])
        if key in lookup:
            duplicates.add(key)
        lookup[key] = {
            'idx': idx,
            'value': row['LatestValue'],
            'maxvolume': row['maxvolume'],
            'maxinhoud': row['maxinhoud'],
            'density': row['density'],
            'tankshape': row['tankshape'],
            'ts': row['LatestTs'],
        }

    if duplicates:
        logger.warning(f"Multiple pct tags found for tanks {sorted(duplicates)}; "
                       f"preferring tag with Niveau Desc, then latest timestamp")

    logger.info(f"Niveau lookup contains {len(lookup)} tanks with a pct value")
    return lookup

# --------------------------------------------------------------------------------------------------------------
# INHOUD (MT) FALLBACK CALCULATION
# --------------------------------------------------------------------------------------------------------------


def compute_inhoud_fallback(niveau_info, row_maxvolume, row_maxinhoud, row_density, row_tankshape, key):
    if niveau_info is None:
        return None, SOURCE_MISSING_NIVEAU

    niveau = pd.to_numeric(niveau_info['value'], errors='coerce')
    maxvolume = coalesce_numeric(row_maxvolume, niveau_info['maxvolume'])
    maxinhoud = coalesce_numeric(row_maxinhoud, niveau_info['maxinhoud'])
    density = coalesce_numeric(row_density, niveau_info['density'])
    tankshape = row_tankshape if normalize_text(row_tankshape) else niveau_info['tankshape']

    if pd.isna(niveau):
        return None, SOURCE_MISSING_NIVEAU

    if niveau < NIVEAU_MIN_PCT or niveau > NIVEAU_MAX_PCT:
        logger.warning(f"Tank {key}: niveau {niveau} outside plausible range, skipping calculation")
        return None, SOURCE_MISSING_NIVEAU

    if not is_vertical(tankshape):
        logger.warning(f"Tank {key}: tankshape '{tankshape}' is not vertical, "
                       f"linear niveau-to-mass calculation skipped")
        return None, SOURCE_SKIPPED_SHAPE

    if pd.notna(maxinhoud) and maxinhoud > 0:
        return maxinhoud * (niveau / 100.0), SOURCE_MAXINHOUD

    if pd.notna(density) and pd.notna(maxvolume) and maxvolume > 0:
        if DENSITY_MIN_T_M3 <= density <= DENSITY_MAX_T_M3:
            return maxvolume * (niveau / 100.0) * density, SOURCE_DENSITY
        logger.warning(f"Tank {key}: density {density} not plausible as t/m3, calculation skipped")

    return None, SOURCE_MISSING_MASTERDATA

# --------------------------------------------------------------------------------------------------------------
# MERGE AND TRANSFORM DATA
# --------------------------------------------------------------------------------------------------------------


def merge_and_transform_data(mestags_df, timeseriedata_df):
    logger.info("Merging dataframes...")

    mestags_df['tag'] = mestags_df['tag'].astype(str).str.strip().str.lower()
    mestags_df['messerver'] = mestags_df['messerver'].astype(str).str.strip().str.lower()
    timeseriedata_df['TagName'] = timeseriedata_df['TagName'].astype(str).str.strip().str.lower()
    timeseriedata_df['MESServer'] = timeseriedata_df['MESServer'].astype(str).str.strip().str.lower()

    matched = mestags_df['tag'].isin(timeseriedata_df['TagName']).sum()
    logger.info(f"Tags matching after normalization: {matched} / {len(mestags_df)}")

    distinct_descs = sorted(mestags_df['Desc'].dropna().astype(str).str.strip().unique())
    logger.info(f"Distinct Desc values in mestags: {distinct_descs}")

    merged_df = pd.merge(
        mestags_df,
        timeseriedata_df,
        left_on=['tag', 'messerver'],
        right_on=['TagName', 'MESServer'],
        how='left'
    ).reset_index(drop=True)

    logger.info(f"Merged data contains {len(merged_df)} records")

    inhoud_mask = desc_mask(merged_df['Desc'], DESC_INHOUD_ALIASES)
    logger.info(f"Rows recognized as Inhoud: {inhoud_mask.sum()}")

    inhoud_desc_values = merged_df.loc[inhoud_mask, 'Desc'].dropna()
    canonical_inhoud_desc = (inhoud_desc_values.mode().iloc[0]
                             if not inhoud_desc_values.empty else DESC_INHOUD_DEFAULT)

    result_df = pd.DataFrame()
    result_df['Tags'] = merged_df['tag']
    result_df['MES Server'] = merged_df['messerver']
    result_df['Units'] = merged_df['UnitOfMeasure']
    result_df['Compression Setting'] = merged_df['compression']
    result_df['Data type'] = merged_df['RecordType']
    result_df['Default'] = merged_df['IP_STEPPED']
    result_df['Tank'] = merged_df['tank']
    result_df['Desc'] = merged_df['Desc']
    result_df['TankCheck'] = merged_df['TankCheck']
    result_df['IP_DESCRIPTION'] = merged_df['IP_DESCRIPTION']
    result_df['IP_INPUT_TIME'] = merged_df['LatestTs']
    result_df['ip_input_value'] = merged_df['LatestValue']
    result_df['IP_ENG_UNITS'] = merged_df['UnitOfMeasure']
    result_df['Calculation'] = result_df['ip_input_value'].copy()

    # ----------------------------------------------------------------------------------------------------------
    # STEP 1: APPLY EXISTING CALCULATION EXPRESSIONS TO MEASURED INHOUD VALUES
    # ----------------------------------------------------------------------------------------------------------
    calculation_mask = (merged_df['calculation'].notna() &
                        (merged_df['calculation'] != '') &
                        inhoud_mask)

    logger.info(f"Applying calculation expressions to {calculation_mask.sum()} Inhoud (MT) rows")
    for idx in merged_df[calculation_mask].index:
        result_df.at[idx, 'Calculation'] = apply_calculation(
            merged_df.at[idx, 'LatestValue'],
            merged_df.at[idx, 'calculation']
        )

    result_df['Calculation'] = pd.to_numeric(result_df['Calculation'], errors='coerce')

    # ----------------------------------------------------------------------------------------------------------
    # STEP 2: MARK MEASURED INHOUD ROWS
    # ----------------------------------------------------------------------------------------------------------
    result_df['InhoudSource'] = None
    measured_mask = inhoud_mask & result_df['Calculation'].notna()
    result_df.loc[measured_mask, 'InhoudSource'] = SOURCE_MEASURED

    # ----------------------------------------------------------------------------------------------------------
    # STEP 3: FILL EMPTY INHOUD ROWS FROM PCT TAG AND TANK MASTER DATA
    # ----------------------------------------------------------------------------------------------------------
    niveau_lookup = build_niveau_lookup(merged_df)

    fallback_mask = inhoud_mask & result_df['Calculation'].isna()
    for idx in result_df[fallback_mask].index:
        key = tank_key(result_df.at[idx, 'MES Server'], result_df.at[idx, 'Tank'])
        value, source = compute_inhoud_fallback(
            niveau_lookup.get(key),
            merged_df.at[idx, 'maxvolume'],
            merged_df.at[idx, 'maxinhoud'],
            merged_df.at[idx, 'density'],
            merged_df.at[idx, 'tankshape'],
            key
        )
        result_df.at[idx, 'Calculation'] = value
        result_df.at[idx, 'InhoudSource'] = source

    result_df['MaxVolume (M3)'] = merged_df['maxvolume']
    result_df['MaxInhoud (MT)'] = merged_df['maxinhoud']
    result_df['Coordinates'] = merged_df['coordinates']
    result_df['StaticProduct'] = merged_df['staticproduct']
    result_df['Tenant'] = merged_df['tenant']
    result_df['Plant'] = merged_df['plant']
    result_df['Area'] = merged_df['area']
    result_df['Contact'] = merged_df['contact']
    result_df['Unnamed: 9'] = None
    result_df['Check if any data for tank (listing only one of the descriptors)?'] = merged_df['TagDescription']
    result_df['GEVI Code'] = merged_df['gevicode']
    result_df['Comment'] = merged_df['comment']
    result_df['filename'] = merged_df['filename']

    # ----------------------------------------------------------------------------------------------------------
    # STEP 4: SYNTHESIZE INHOUD ROWS FOR TANKS WITH A PCT TAG BUT NO INHOUD TAG
    # ----------------------------------------------------------------------------------------------------------
    result_df = synthesize_missing_inhoud_rows(
        result_df, niveau_lookup, inhoud_mask, canonical_inhoud_desc
    )

    # ----------------------------------------------------------------------------------------------------------
    # STEP 5: LOG SOURCE BREAKDOWN AND MISSING-VALUE DIAGNOSTICS
    # ----------------------------------------------------------------------------------------------------------
    final_inhoud_mask = desc_mask(result_df['Desc'], DESC_INHOUD_ALIASES)
    source_counts = result_df.loc[final_inhoud_mask, 'InhoudSource'].value_counts(dropna=False)
    logger.info(f"Inhoud (MT) source breakdown:\n{source_counts.to_string()}")

    log_missing_inhoud_diagnostics(result_df, niveau_lookup, final_inhoud_mask)

    return result_df

# --------------------------------------------------------------------------------------------------------------
# SYNTHESIZE MISSING INHOUD ROWS
# --------------------------------------------------------------------------------------------------------------


def synthesize_missing_inhoud_rows(result_df, niveau_lookup, inhoud_mask, canonical_inhoud_desc):
    existing_keys = {tank_key(ms, tk) for ms, tk in
                     zip(result_df.loc[inhoud_mask, 'MES Server'], result_df.loc[inhoud_mask, 'Tank'])}

    new_rows = []
    for key, info in niveau_lookup.items():
        if key in existing_keys:
            continue

        value, source = compute_inhoud_fallback(
            info, info['maxvolume'], info['maxinhoud'], info['density'], info['tankshape'], key
        )
        if value is None:
            logger.info(f"Tank {key}: has pct tag but no Inhoud row could be synthesized ({source})")
            continue

        row = result_df.loc[info['idx']].copy()
        row['Desc'] = canonical_inhoud_desc
        row['Tags'] = f"{row['Tags']}_inhoud_calc"
        row['ip_input_value'] = None
        row['IP_ENG_UNITS'] = 'MT'
        row['Calculation'] = value
        row['InhoudSource'] = source
        new_rows.append(row)

    if new_rows:
        logger.info(f"Synthesized {len(new_rows)} Inhoud (MT) rows for tanks without an Inhoud tag")
        result_df = pd.concat([result_df, pd.DataFrame(new_rows)], ignore_index=True)

    return result_df

# --------------------------------------------------------------------------------------------------------------
# MISSING INHOUD DIAGNOSTICS
# --------------------------------------------------------------------------------------------------------------


def log_missing_inhoud_diagnostics(result_df, niveau_lookup, final_inhoud_mask):
    missing_mask = final_inhoud_mask & result_df['Calculation'].isna()
    if not missing_mask.any():
        logger.info("All Inhoud (MT) rows have a value")
        return

    diag_rows = []
    for idx in result_df[missing_mask].index:
        key = tank_key(result_df.at[idx, 'MES Server'], result_df.at[idx, 'Tank'])
        info = niveau_lookup.get(key)
        diag_rows.append({
            'Tank': result_df.at[idx, 'Tank'],
            'MES Server': result_df.at[idx, 'MES Server'],
            'Reason': result_df.at[idx, 'InhoudSource'],
            'PctValueFound': info is not None,
            'PctValue': info['value'] if info else None,
            'MaxVolume': result_df.at[idx, 'MaxVolume (M3)'],
            'MaxInhoud': result_df.at[idx, 'MaxInhoud (MT)'],
        })

    diag_df = pd.DataFrame(diag_rows)
    logger.warning(f"MISSING INHOUD DIAGNOSTIC — {len(diag_df)} Inhoud rows without a value:\n"
                   f"{diag_df.to_string(index=False)}")

# --------------------------------------------------------------------------------------------------------------
# SAVE TO EXCEL FILES
# --------------------------------------------------------------------------------------------------------------


def save_to_excel_files(df):
    save_folder = os.getenv('SAVE_FOLDER')
    if not save_folder:
        raise ValueError("SAVE_FOLDER environment variable is not set")

    Path(save_folder).mkdir(parents=True, exist_ok=True)

    grouped = df.groupby('filename')

    logger.info(f"Creating {len(grouped)} Excel files...")

    for filename, group_df in grouped:
        if pd.isna(filename) or filename == '':
            logger.warning("Skipping group with empty filename")
            continue

        output_df = group_df.drop('filename', axis=1)

        excel_filename = f"{filename}.xlsx" if not str(filename).endswith('.xlsx') else filename
        file_path = os.path.join(save_folder, excel_filename)

        try:
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                output_df.to_excel(writer, sheet_name='result_plus', index=False)

            logger.info(f"Created Excel file: {file_path} with {len(output_df)} records")

        except Exception as e:
            logger.error(f"Error creating Excel file {file_path}: {e}")

# --------------------------------------------------------------------------------------------------------------
# MAIN EXECUTION
# --------------------------------------------------------------------------------------------------------------


def main():
    try:
        logger.info("Starting data export process...")

        mestags_df, timeseriedata_df = load_data_from_sql()

        result_df = merge_and_transform_data(mestags_df, timeseriedata_df)

        save_to_excel_files(result_df)

        logger.info("Data export completed successfully!")

    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise


# --------------------------------------------------------------------------------------------------------------
# ENTRY POINT
# --------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    main()
