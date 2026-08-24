# db_to_excel.py
"""
This script connects to an Azure SQL database, retrieves data from two tables,
merges and transforms the data, and saves the results into separate Excel files
based on a filename column. It handles authentication using Azure Identity and
environment variables for configuration.
"""

import os
import pandas as pd
import struct
import pyodbc as db
import sqlalchemy as dbo
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv
import logging
from pathlib import Path

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment version - adjust as needed
ENV_VERSION = os.getenv('ENV_VERSION', 'DEV')

class DBConnection:
    """Database connection class for Azure SQL"""
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


def apply_calculation(latest_value, calculation_text):
    """Apply calculation expression to LatestValue"""
    try:
        # Convert LatestValue to numeric
        value = pd.to_numeric(latest_value, errors='coerce')

        # If value is NaN or calculation_text is None/empty, return NaN
        if pd.isna(value) or pd.isna(calculation_text) or calculation_text == '':
            return None

        # Clean up the calculation text and replace European decimal separators
        calc_str = str(calculation_text).strip().replace(',', '.')

        # Handle different calculation formats
        if calc_str.startswith('/'):
            # Remove the leading '/'
            expression = calc_str[1:]

            # Check if it's a simple division or has additional operations
            if '+' in expression or '-' in expression or '*' in expression:
                # Handle complex expressions like /10000+1
                # This should be interpreted as: (value / 10000) + 1
                # Split by + or - to handle the operations
                if '+' in expression:
                    parts = expression.split('+')
                    divisor = float(parts[0])
                    addend = float(parts[1])
                    return (value / divisor) + addend
                elif '-' in expression:
                    parts = expression.split('-')
                    divisor = float(parts[0])
                    subtrahend = float(parts[1])
                    return (value / divisor) - subtrahend
                else:
                    # Contains *, treat as expression to evaluate
                    divisor_expression = eval(expression)
                    return value / divisor_expression
            else:
                # Simple division
                divisor = float(expression)
                return value / divisor

        elif calc_str.startswith('*'):
            # Format: *720*853/1000000 -> multiply then divide
            # Remove the first * and evaluate the rest
            expression = calc_str[1:]
            # Replace *** with * for cases like ***720*853/1000000
            expression = expression.replace('***', '*').replace('**', '*')
            # Safely evaluate the mathematical expression
            result = eval(expression)
            return value * result

        elif '/' in calc_str or '*' in calc_str:
            # Handle direct mathematical expressions
            expression = calc_str.replace('***', '*').replace('**', '*')
            result = eval(expression)
            return value * result
        else:
            # Try to treat as a simple multiplier
            multiplier = float(calc_str)
            return value * multiplier

    except (ValueError, ZeroDivisionError, SyntaxError, NameError) as e:
        logger.warning(f"Error applying calculation '{calculation_text}' to value '{latest_value}': {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error in calculation: {e}")
        return None


def load_data_from_sql():
    """Load data from both SQL tables"""
    logger.info("Establishing database connection...")
    db_conn = DBConnection()
    engine = db_conn.get_engine()

    try:
        # Query for mestags table
        mestags_query = """
        SELECT [tag], [messerver], [tank], [compression], [Desc], [TankCheck], 
               [IP_DESCRIPTION], [maxvolume], [maxinhoud], [coordinates], 
               [staticproduct], [tenant], [plant], [area], [contact], 
               [staticdesc], [gevicode], [comment], [filename], [calculation]
        FROM [mes].[mestags]
        """

        # Query for timeseriedata table - only fetch the columns we actually need
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


def merge_and_transform_data(mestags_df, timeseriedata_df):
    """Merge the dataframes and transform according to requirements"""
    logger.info("Merging dataframes...")

    # Normalize join keys: strip whitespace and lowercase to avoid mismatches
    mestags_df['tag'] = mestags_df['tag'].astype(str).str.strip().str.lower()
    mestags_df['messerver'] = mestags_df['messerver'].astype(str).str.strip().str.lower()
    timeseriedata_df['TagName'] = timeseriedata_df['TagName'].astype(str).str.strip().str.lower()
    timeseriedata_df['MESServer'] = timeseriedata_df['MESServer'].astype(str).str.strip().str.lower()

    # Debug: log how many tags actually match
    matched = mestags_df['tag'].isin(timeseriedata_df['TagName']).sum()
    logger.info(f"Tags matching after normalization: {matched} / {len(mestags_df)}")

    # Use LEFT join so mestags is the base — all mestags rows are kept
    merged_df = pd.merge(
        mestags_df, 
        timeseriedata_df,
        left_on=['tag', 'messerver'],
        right_on=['TagName', 'MESServer'],
        how='left'
    )

    logger.info(f"Merged data contains {len(merged_df)} records")

    # Create the final dataframe with required columns
    result_df = pd.DataFrame()

    # All columns come from mestags (the base table) ...
    result_df['Tags'] = merged_df['tag']
    result_df['MES Server'] = merged_df['messerver']
    result_df['Units'] = merged_df['UnitOfMeasure']
    result_df['Compression Setting'] = merged_df['compression']
    result_df['Data type'] = merged_df['RecordType']
    result_df['Default'] = merged_df['IP_STEPPED']
    result_df['Tank'] = merged_df['tank']
    result_df['Desc'] = merged_df['Desc']
    result_df['TankCheck'] = merged_df['TankCheck']
    result_df['IP_DESCRIPTION'] = merged_df['IP_DESCRIPTION']  # from mestags
    # ... EXCEPT these 3 columns which come from timeseriedata
    result_df['IP_INPUT_TIME'] = merged_df['LatestTs']
    result_df['ip_input_value'] = merged_df['LatestValue']
    result_df['IP_ENG_UNITS'] = merged_df['UnitOfMeasure']

    # Initialize Calculation column as a copy of ip_input_value
    result_df['Calculation'] = result_df['ip_input_value'].copy()

    # Apply calculations only for rows that have a value in the calculation column AND Desc = 'Inhoud (MT)'
    has_calculation = (merged_df['calculation'].notna() & 
                      (merged_df['calculation'] != '') & 
                      (merged_df['Desc'] == 'Inhoud (MT)'))

    if has_calculation.any():
        logger.info(f"Applying calculations to {has_calculation.sum()} rows with calculation values and Desc='Inhoud (MT)'")

        # Reset index to ensure alignment between merged_df and result_df
        merged_df_reset = merged_df.reset_index(drop=True)
        result_df = result_df.reset_index(drop=True)

        # Apply calculation to rows that have calculation values AND Desc = 'Inhoud (MT)'
        calculation_mask = (merged_df_reset['calculation'].notna() & 
                           (merged_df_reset['calculation'] != '') & 
                           (merged_df_reset['Desc'] == 'Inhoud (MT)'))

        for i, (idx, row) in enumerate(merged_df_reset[calculation_mask].iterrows()):
            latest_value = row['LatestValue']
            calculation_text = row['calculation']

            calculated_value = apply_calculation(latest_value, calculation_text)

            # Update the Calculation column with the calculated value
            result_df.loc[idx, 'Calculation'] = calculated_value

    result_df['MaxVolume (M3)'] = merged_df['maxvolume']
    result_df['MaxInhoud (MT)'] = merged_df['maxinhoud']
    result_df['Coordinates'] = merged_df['coordinates']
    result_df['StaticProduct'] = merged_df['staticproduct']
    result_df['Tenant'] = merged_df['tenant']
    result_df['Plant'] = merged_df['plant']
    result_df['Area'] = merged_df['area']
    result_df['Contact'] = merged_df['contact']
    result_df['Unnamed: 9'] = None  # Empty column
    result_df['Check if any data for tank (listing only one of the descriptors)?'] = merged_df['TagDescription']
    result_df['GEVI Code'] = merged_df['gevicode']
    result_df['Comment'] = merged_df['comment']

    # Add filename for grouping
    result_df['filename'] = merged_df['filename']

    return result_df


def save_to_excel_files(df):
    """Save data grouped by filename to separate Excel files"""
    save_folder = os.getenv('SAVE_FOLDER')
    if not save_folder:
        raise ValueError("SAVE_FOLDER environment variable is not set")

    # Ensure save folder exists
    Path(save_folder).mkdir(parents=True, exist_ok=True)

    # Group by filename
    grouped = df.groupby('filename')

    logger.info(f"Creating {len(grouped)} Excel files...")

    for filename, group_df in grouped:
        if pd.isna(filename) or filename == '':
            logger.warning("Skipping group with empty filename")
            continue

        # Remove the filename column from the output (it was just for grouping)
        output_df = group_df.drop('filename', axis=1)

        # Create full file path
        excel_filename = f"{filename}.xlsx" if not filename.endswith('.xlsx') else filename
        file_path = os.path.join(save_folder, excel_filename)

        try:
            # Write to Excel with sheet name 'result_plus'
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                output_df.to_excel(writer, sheet_name='result_plus', index=False)

            logger.info(f"Created Excel file: {file_path} with {len(output_df)} records")

        except Exception as e:
            logger.error(f"Error creating Excel file {file_path}: {e}")


def main():
    """Main execution function"""
    try:
        logger.info("Starting data export process...")

        # Load data from SQL
        mestags_df, timeseriedata_df = load_data_from_sql()

        # Merge and transform data
        result_df = merge_and_transform_data(mestags_df, timeseriedata_df)

        # Save to Excel files
        save_to_excel_files(result_df)

        logger.info("Data export completed successfully!")

    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise


if __name__ == "__main__":
    main()
