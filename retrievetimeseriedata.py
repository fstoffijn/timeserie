# retrievetimeseriedata.py

"""Retrieve and process time series data from Azure Data Explorer (ADX) and store it in Azure SQL Database."""

# --------------------------------------------------------------------------------------------------------------
# Required Libraries
# --------------------------------------------------------------------------------------------------------------

import pandas as pd
import numpy as np
import sqlalchemy as dbo
import pyodbc as db
from datetime import datetime
from sqlalchemy import create_engine, text, DECIMAL
from sqlalchemy.dialects.mssql import DATETIME2, NVARCHAR
from dotenv import load_dotenv
from loguru import logger
import struct
import os
import sys
import unicodedata
import re
import json

# --------------------------------------------------------------------------------------------------------------
# Configuration and Environment Setup
# --------------------------------------------------------------------------------------------------------------

# Load environment variables
load_dotenv()

# Determine if using Device Code authentication (e.g., on VM)
DVC = os.getenv('DVC', 'False').lower() == 'true'

# Normal import for Kusto client
if DVC == False:
    from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
    from azure.identity import DefaultAzureCredential

# For interactive auth during local testing, uncomment the following line:
if DVC == True:
    from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
    from azure.identity import DeviceCodeCredential

DATABASE_NAME = os.getenv('DATABASE_NAME')
ADX_CLUSTER = os.getenv('ADX_CLUSTER')
ENV_VERSION = os.getenv('ENV_VERSION', 'DEV')
DEBUG_MODE = os.getenv('DEBUG_MODE', 'False').lower() == 'true'
TENANT_ID = os.getenv('TENANT_ID')

# Configuration constants
DEFAULT_CHUNK_SIZE = 50
DEFAULT_POOL_SIZE = 5
DEFAULT_MAX_OVERFLOW = 2
DEFAULT_TIMEOUT = 30
DEFAULT_POOL_RECYCLE = 3600

# Simple logger setup
logger.remove()
logger.add(sys.stdout, level="INFO")

# --------------------------------------------------------------------------------------------------------------
# Security and Utility Functions
# --------------------------------------------------------------------------------------------------------------

def sanitize_error_message(error_msg: str) -> str:
    """Remove sensitive information from error messages"""
    sanitized = str(error_msg)
    # Remove connection strings, server names, passwords
    sanitized = re.sub(r'(Server=)[^;]*', r'\1***', sanitized)
    sanitized = re.sub(r'(Password=)[^;]*', r'\1***', sanitized)
    sanitized = re.sub(r'(Pwd=)[^;]*', r'\1***', sanitized)
    sanitized = re.sub(r'(server=)[^;]*', r'\1***', sanitized)
    sanitized = re.sub(r'(Data Source=)[^;]*', r'\1***', sanitized)
    sanitized = re.sub(r'(Initial Catalog=)[^;]*', r'\1***', sanitized)
    return sanitized

def log_error_safely(error: Exception, context: str = ""):
    """Log errors safely based on environment"""
    if DEBUG_MODE:
        logger.error(f"Error in {context}: {error}")
        logger.error(f"Stack trace: {sys.exc_info()}")
    else:
        # Production logging - minimal details
        sanitized_msg = sanitize_error_message(str(error))
        logger.error(f"Error in {context}: {sanitized_msg}")

def validate_server_filter(server_filter):
    """Validate server filter to prevent KQL injection"""
    if server_filter is None:
        return None
    
    if isinstance(server_filter, str):
        # Only allow alphanumeric, hyphens, underscores, dots
        if not re.match(r'^[a-zA-Z0-9._-]+$', server_filter):
            raise ValueError("Invalid server filter format")
        return server_filter
    elif isinstance(server_filter, list):
        for server in server_filter:
            if not isinstance(server, str) or not re.match(r'^[a-zA-Z0-9._-]+$', server):
                raise ValueError("Invalid server filter format")
        return server_filter
    else:
        raise ValueError("Server filter must be string or list")

# --------------------------------------------------------------------------------------------------------------
# Database Connection Class
# --------------------------------------------------------------------------------------------------------------

class DBConnection:
    """Database connection handler with enhanced security"""
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
            logger.error("Missing required environment variables for database connection")
            raise ValueError("Database configuration incomplete. Check environment variables.")
        
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
            log_error_safely(e, "database connection")
            raise ValueError("Failed to establish database connection")

    def get_engine(self):
        return dbo.create_engine(
            'mssql+pyodbc://', 
            creator=lambda: self.get_connection(), 
            pool_size=DEFAULT_POOL_SIZE, 
            max_overflow=DEFAULT_MAX_OVERFLOW,
            pool_timeout=DEFAULT_TIMEOUT,
            pool_recycle=DEFAULT_POOL_RECYCLE
        )

# --------------------------------------------------------------------------------------------------------------
# ADX Connection and Data Retrieval
# --------------------------------------------------------------------------------------------------------------


def connect_to_adx() -> KustoClient:
    #Establish a secure connection to Azure Data Explorer (ADX) use this if not on VM
    if DVC == False:
        try:
            if not ADX_CLUSTER:
                raise ValueError("ADX_CLUSTER environment variable not set")
            credential = DefaultAzureCredential()
            kcsb = KustoConnectionStringBuilder.with_azure_token_credential(ADX_CLUSTER, credential)
            client = KustoClient(kcsb)
            return client
        except Exception as e:
            log_error_safely(e, "ADX connection")
            raise

    #Establish a secure connection to Azure Data Explorer (ADX) use this if on VM
    elif DVC == True:
        try:
            cred = DeviceCodeCredential(tenant_id=TENANT_ID)
            kcsb = KustoConnectionStringBuilder.with_azure_token_credential(ADX_CLUSTER, cred)
            client = KustoClient(kcsb)
            print(client.execute(DATABASE_NAME, ".show version").primary_results[0][0])
        except Exception as e:
            log_error_safely(e, "ADX connection")
            raise

def dataframe_from_result_table(result_table) -> pd.DataFrame:
    columns = [col.column_name for col in result_table.columns]
    data = [[row[i] for i in range(len(columns))] for row in result_table]
    return pd.DataFrame(data, columns=columns)

def clean_special_characters(text_value):
    """Clean special characters that cause SQL insertion issues"""
    if pd.isna(text_value) or text_value is None:
        return None
    
    text_str = str(text_value)
    
    # Common unit character replacements for engineering/MES data
    replacements = {
        '°': 'deg',
        '°C': 'degC',
        '°F': 'degF',
        '%': 'pct',
        'µ': 'micro',
        'Ω': 'ohm',
        '²': '2',
        '³': '3',
        '⁻': '-',
        '⁺': '+',
        'Δ': 'delta',
        'α': 'alpha',
        'β': 'beta',
        'γ': 'gamma',
        'λ': 'lambda',
        'π': 'pi',
        '∞': 'inf',
        '±': '+/-',
        '≤': '<=',
        '≥': '>=',
        '≠': '!=',
        '×': 'x',
        '÷': '/',
        '√': 'sqrt',
        '∫': 'int',
        '∑': 'sum',
        '∆': 'delta',
        '№': 'No', 
        '℃': 'degC',
        '℉': 'degF', 
    }
    
    # Apply replacements
    for original, replacement in replacements.items():
        text_str = text_str.replace(original, replacement)
    
    # Remove or replace any remaining non-ASCII characters
    try:
        text_str = unicodedata.normalize('NFKD', text_str)
        text_str = text_str.encode('ascii', 'ignore').decode('ascii')
    except:
        # If that fails, remove non-ASCII
        text_str = re.sub(r'[^\x00-\x7F]+', '', text_str)
    
    # Clean up extra spaces
    text_str = ' '.join(text_str.split())
    
    return text_str if text_str else None

# --------------------------------------------------------------------------------------------------------------
# JSON Property Extraction
# --------------------------------------------------------------------------------------------------------------

def extract_json_properties(df: pd.DataFrame) -> pd.DataFrame:
    """Extract specific properties from JSON Properties column using pandas"""
    logger.info("Extracting JSON properties...")
    
    if 'Properties' not in df.columns:
        logger.warning("Properties column not found in dataframe")
        return df
    
    df_with_properties = df.copy()
    
    # Initialize new columns with None
    property_columns = ['IP_PLANT_AREA', 'IP_STEPPED', 'IP_GRAPH_MAXIMUM', 'IP_GRAPH_MINIMUM', 'IP_DESCRIPTION']
    for col in property_columns:
        df_with_properties[col] = None
    
    # Extract properties for each row
    extracted_count = 0
    for idx, row in df_with_properties.iterrows():
        if pd.notna(row['Properties']) and row['Properties']:
            try:
                # Parse JSON
                if isinstance(row['Properties'], str):
                    properties_dict = json.loads(row['Properties'])
                elif isinstance(row['Properties'], dict):
                    properties_dict = row['Properties']
                else:
                    continue
                
                # Extract specific values
                df_with_properties.at[idx, 'IP_PLANT_AREA'] = properties_dict.get('IP_PLANT_AREA', None)
                df_with_properties.at[idx, 'IP_STEPPED'] = properties_dict.get('IP_STEPPED', None)
                df_with_properties.at[idx, 'IP_GRAPH_MAXIMUM'] = properties_dict.get('IP_GRAPH_MAXIMUM', None)
                df_with_properties.at[idx, 'IP_GRAPH_MINIMUM'] = properties_dict.get('IP_GRAPH_MINIMUM', None)
                df_with_properties.at[idx, 'IP_DESCRIPTION'] = properties_dict.get('IP_DESCRIPTION', None)
                extracted_count += 1
                
            except (json.JSONDecodeError, TypeError, AttributeError) as e:
                if DEBUG_MODE:
                    logger.debug(f"Failed to parse Properties for RecordId {row.get('RecordId', 'unknown')}: {e}")
                continue
    
    # Clean the extracted numeric values
    for col in ['IP_GRAPH_MAXIMUM', 'IP_GRAPH_MINIMUM']:
        if col in df_with_properties.columns:
            # Convert to numeric, removing any extra spaces
            df_with_properties[col] = df_with_properties[col].astype(str).str.strip()
            df_with_properties[col] = pd.to_numeric(df_with_properties[col], errors='coerce')
    
    # Clean text values
    for col in ['IP_PLANT_AREA', 'IP_STEPPED']:
        if col in df_with_properties.columns:
            df_with_properties[col] = df_with_properties[col].astype(str).str.strip()
            df_with_properties[col] = df_with_properties[col].replace('nan', None)
    
    logger.info(f"Extracted properties for {extracted_count} records")
    return df_with_properties

# --------------------------------------------------------------------------------------------------------------
# Data Processing and Cleaning
# --------------------------------------------------------------------------------------------------------------

def clean_and_prepare_data(df: pd.DataFrame, include_properties: bool = True) -> pd.DataFrame:
    """Clean data with special character handling and optional JSON property extraction"""
    logger.info("Cleaning and preparing data...")
    df_clean = df.copy()
    
    # Fix duplicate column names
    df_clean.columns = [f"{col}_{i}" if col in df_clean.columns[:i] else col 
                       for i, col in enumerate(df_clean.columns)]
    
    # Replace inf/-inf with NaN
    df_clean = df_clean.replace([np.inf, -np.inf], np.nan)
    
    # Identify text columns that likely contain special characters
    text_columns_with_special_chars = [
        'UnitOfMeasure', 'TagDescription', 'LatestQuality', 
        'LatestUom', 'DataSourceName', 'MESServer', 'TagName',
        'RecordName', 'FullRecordName', 'RecordType'
    ]
    
    # Clean special characters in text columns
    for col in text_columns_with_special_chars:
        if col in df_clean.columns:
            logger.info(f"Cleaning special characters in column: {col}")
            
            # Show sample of original data if in debug mode
            if DEBUG_MODE:
                sample_before = df_clean[col].dropna().head(3).tolist()
                logger.debug(f"  Sample before: {sample_before}")
            
            # Clean the column
            df_clean[col] = df_clean[col].apply(clean_special_characters)
            
            # Show sample of cleaned data if in debug mode
            if DEBUG_MODE:
                sample_after = df_clean[col].dropna().head(3).tolist()
                logger.debug(f"  Sample after: {sample_after}")
    
    # Handle datetime columns
    timestamp_columns = ['LatestTs', 'LatestTsLocal']
    for col in timestamp_columns:
        if col in df_clean.columns:
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
            df_clean[col] = df_clean[col].where(pd.notna(df_clean[col]), None)
    
    # Handle boolean columns
    if 'IsStepped' in df_clean.columns:
        df_clean['IsStepped'] = df_clean['IsStepped'].astype('boolean')
    
    # Handle numeric columns
    numeric_columns = ['LatestValue', 'LatestTextValue', 'LatestTzOffset']
    for col in numeric_columns:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
            df_clean[col] = df_clean[col].replace([np.inf, -np.inf], None)
    
    # Extract JSON properties if requested
    if include_properties and 'Properties' in df_clean.columns:
        df_clean = extract_json_properties(df_clean)
        # Drop the original Properties column to avoid SQL insertion issues
        df_clean = df_clean.drop('Properties', axis=1)
        logger.info("Dropped original Properties column after extraction")
    
    logger.info(f"Data cleaning complete. Shape: {df_clean.shape}")
    return df_clean

# --------------------------------------------------------------------------------------------------------------
# Data Retrieval Functions
# --------------------------------------------------------------------------------------------------------------

def get_comprehensive_mes_data(client: KustoClient, database_name: str = DATABASE_NAME, 
                              server_filter=None, include_properties: bool = True) -> pd.DataFrame:
    """Retrieve comprehensive MES data with optional properties column"""
    
    # Validate server filter for security
    server_filter = validate_server_filter(server_filter)
    
    # Build server filter securely
    server_where_clause = ""
    if server_filter:
        if isinstance(server_filter, str):
            server_where_clause = f'| where DataSourceName == "{server_filter}"'
        elif isinstance(server_filter, list):
            server_list = '", "'.join(server_filter)
            server_where_clause = f'| where DataSourceName in ("{server_list}")'
    
    # Build properties projection based on requirement
    properties_projection = "Properties," if include_properties else ""
    
    query = f"""
    let LatestValues = mvTimeSeries
    {server_where_clause}
    | summarize arg_max(Ts, Value, TextValue, Quality, Uom, TsLocal, TzOffset) by RecordId
    | project RecordId, 
              LatestTs = Ts, 
              LatestValue = Value, 
              LatestTextValue = TextValue, 
              LatestQuality = Quality, 
              LatestUom = Uom,
              LatestTsLocal = TsLocal,
              LatestTzOffset = TzOffset;
    
    // Get tag metadata
    mvTags
    {server_where_clause}
    | where Exists == true
    | join kind=leftouter LatestValues on RecordId
    | extend 
        // Split FullRecordName into MESServer and TagName
        MESServer = tostring(split(FullRecordName, ".")[0]),
        TagName = tostring(split(FullRecordName, ".")[1])
    
    | project 
        // Basic identifiers
        RecordId,
        DataSourceName,
        MESServer,
        TagName,
        RecordName,
        FullRecordName,
        RecordType,
        
        // Tag metadata (from mvTags table)
        TagDescription = Description,
        IsStepped,
        UnitOfMeasure,
        {properties_projection}
        
        // Latest values from mvTimeSeries
        LatestTs,
        LatestValue,
        LatestTextValue,
        LatestQuality,
        LatestUom,
        LatestTsLocal,
        LatestTzOffset
    
    | order by DataSourceName asc, TagName asc
    """
    
    query_type = "with Properties" if include_properties else "without Properties"
    logger.info(f"Executing comprehensive MES data query ({query_type})...")
    
    try:
        response = client.execute(database_name, query)
        df = dataframe_from_result_table(response.primary_results[0])
        logger.info(f"Retrieved {len(df)} records")
        return df
    except Exception as e:
        log_error_safely(e, "ADX query execution")
        raise

# --------------------------------------------------------------------------------------------------------------
# SQL Database Operations
# --------------------------------------------------------------------------------------------------------------

def write_mes_data_to_sql(df: pd.DataFrame, table_name: str = "mes_comprehensive_data") -> None:
    """Write MES data to SQL with enhanced security and error handling"""
    try:
        if df.empty:
            logger.warning("No data to write to SQL")
            return
        
        # Get database connection
        db_conn = DBConnection()
        engine = db_conn.get_engine()
        
        logger.info(f"Writing {len(df):,} rows to SQL table '{table_name}'...")
        
        with engine.connect() as conn:
            try:
                # Drop table if exists
                drop_statement = text(f"""
                    IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[mes].[{table_name}]') AND type in (N'U'))
                    DROP TABLE [mes].[{table_name}]
                """)
                conn.execute(drop_statement)
                conn.commit()
                logger.info(f"Dropped existing table mes.{table_name} if it existed")
                
                # Define data type overrides
                dtype_overrides = {}
                
                # Datetime columns
                datetime_columns = ['LatestTs', 'LatestTsLocal']
                for col in datetime_columns:
                    if col in df.columns:
                        dtype_overrides[col] = DATETIME2
                
                # Text columns that might have special characters - use NVARCHAR
                text_columns = ['UnitOfMeasure', 'TagDescription', 'LatestQuality', 
                               'LatestUom', 'DataSourceName', 'MESServer', 'TagName',
                               'RecordName', 'FullRecordName', 'RecordType', 
                               'IP_PLANT_AREA', 'IP_STEPPED']
                
                for col in text_columns:
                    if col in df.columns:
                        dtype_overrides[col] = NVARCHAR(500)
                
                # Numeric columns including extracted property columns
                numeric_columns = ['IP_GRAPH_MAXIMUM', 'IP_GRAPH_MINIMUM']
                for col in numeric_columns:
                    if col in df.columns:
                        dtype_overrides[col] = DECIMAL(10, 3)
                
                logger.info(f"Using data type overrides for {len(dtype_overrides)} columns")
                
                # Try insertion with proper Unicode handling
                df.to_sql(
                    table_name,
                    conn,
                    schema='mes',
                    if_exists='replace',
                    index=False,
                    dtype=dtype_overrides,
                    chunksize=DEFAULT_CHUNK_SIZE,
                    method='multi'
                )
                
                # Verify the insert
                verify_statement = text(f"SELECT COUNT(*) FROM [mes].[{table_name}]")
                result = conn.execute(verify_statement)
                row_count = result.fetchone()[0]
                logger.success(f"✅ Successfully wrote {row_count:,} rows to mes.{table_name}")
                
                # Log property extraction statistics if properties were included
                prop_cols = ['IP_PLANT_AREA', 'IP_STEPPED', 'IP_GRAPH_MAXIMUM', 'IP_GRAPH_MINIMUM']
                for col in prop_cols:
                    if col in df.columns:
                        non_null_count = df[col].notna().sum()
                        logger.info(f"  {col}: {non_null_count:,} non-null values ({non_null_count/len(df)*100:.1f}%)")
                
            except Exception as e:
                logger.warning("Standard insertion failed, trying fallback with smaller chunks...")
                try:
                    # Fallback: smaller chunks, no multi method
                    df.to_sql(
                        table_name,
                        conn,
                        schema='mes',
                        if_exists='replace',
                        index=False,
                        dtype=dtype_overrides,
                        chunksize=10,
                        method=None
                    )
                    
                    verify_statement = text(f"SELECT COUNT(*) FROM [mes].[{table_name}]")
                    result = conn.execute(verify_statement)
                    row_count = result.fetchone()[0]
                    logger.success(f"✅ Fallback successful: {row_count:,} rows written to mes.{table_name}")
                    
                except Exception as e2:
                    log_error_safely(e2, "SQL insertion (all methods failed)")
                    
                    # Save to CSV as last resort
                    csv_filename = f'{table_name}_failed_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
                    df.to_csv(csv_filename, index=False, encoding='utf-8')
                    logger.info(f"Data saved to CSV: {csv_filename}")
                    raise
                
    except Exception as e:
        log_error_safely(e, "write_mes_data_to_sql")
        raise

# --------------------------------------------------------------------------------------------------------------
# Data Analysis Functions
# --------------------------------------------------------------------------------------------------------------

def analyze_comprehensive_data(df: pd.DataFrame) -> None:
    """Analyze the comprehensive MES data"""
    if df.empty:
        logger.error("No data retrieved!")
        return
    
    logger.info(f"\n=== MES DATA ANALYSIS ===")
    logger.info(f"Total tags: {len(df):,}")
    logger.info(f"Number of MES servers: {df['MESServer'].nunique()}")
    logger.info(f"Number of data sources: {df['DataSourceName'].nunique()}")
    
    # Tags with recent data
    recent_data = df[df['LatestTs'].notna()]
    logger.info(f"Tags with recent data: {len(recent_data):,} ({len(recent_data)/len(df)*100:.1f}%)")
    
    if len(recent_data) > 0:
        logger.info(f"Latest data timestamp: {recent_data['LatestTs'].max()}")
        logger.info(f"Oldest recent data: {recent_data['LatestTs'].min()}")

# --------------------------------------------------------------------------------------------------------------
# Utility Functions
# --------------------------------------------------------------------------------------------------------------

def get_servers_list(client: KustoClient, database_name: str = DATABASE_NAME) -> pd.DataFrame:
    """Get list of all MES servers with tag counts"""
    query = """
    mvTags
    | where Exists == true
    | summarize 
        TagCount = count(),
        SampleTags = make_set(FullRecordName, 3)
    by DataSourceName
    | order by TagCount desc
    """
    
    try:
        response = client.execute(database_name, query)
        return dataframe_from_result_table(response.primary_results[0])
    except Exception as e:
        log_error_safely(e, "get_servers_list")
        raise

# --------------------------------------------------------------------------------------------------------------
# Main Execution Functions
# --------------------------------------------------------------------------------------------------------------

def main(include_properties: bool = True, server_filter=None):
    """Main execution function with enhanced security and flexibility"""
    try:
        # Connect to ADX
        logger.info("Connecting to Azure Data Explorer...")
        client = connect_to_adx()
        
        # Get list of available servers
        logger.info("\n=== AVAILABLE MES SERVERS ===")
        servers_df = get_servers_list(client)
        logger.info(f"\n{servers_df.to_string()}")

        # Get comprehensive data
        properties_text = "with JSON properties" if include_properties else "without properties"
        logger.info(f"\n=== RETRIEVING COMPREHENSIVE MES DATA ({properties_text}) ===")
        comprehensive_data = get_comprehensive_mes_data(client, server_filter=server_filter, 
                                                       include_properties=include_properties)
        
        if comprehensive_data.empty:
            logger.warning("No data retrieved from ADX")
            return
        
        # Analyze the data
        analyze_comprehensive_data(comprehensive_data)
        
        # Clean and prepare data
        logger.info(f"\n=== CLEANING AND PREPARING DATA ===")
        cleaned_data = clean_and_prepare_data(comprehensive_data, include_properties=include_properties)
        
        # Save to CSV first (as backup)
        csv_filename = f'comprehensive_mes_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        cleaned_data.to_csv(csv_filename, index=False, encoding='utf-8')
        logger.info(f"Data saved to {csv_filename}")

        # Write to SQL database
        #table_suffix = "with_properties" if include_properties else "standard"
        table_name = f"timeseriedata"
        
        logger.info(f"\n=== WRITING TO SQL DATABASE ===")
        write_mes_data_to_sql(cleaned_data, table_name)
        
        logger.success("🎉 MES data processing completed successfully!")

    except Exception as e:
        log_error_safely(e, "main execution")
        raise

# --------------------------------------------------------------------------------------------------------------
# Entry Point
# --------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    # Configuration options:
    
    # Option 1: Include JSON properties extraction (recommended)
    main(include_properties=True)
    
    # Option 2: Without properties (faster, smaller dataset)
    # main(include_properties=False)
    
    # Option 3: Filter by specific server(s)
    # main(include_properties=True, server_filter="YOUR_SERVER_NAME")
    # main(include_properties=True, server_filter=["SERVER1", "SERVER2"])
