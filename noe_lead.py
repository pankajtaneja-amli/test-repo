"""
NeoLead Data Processing Module

This module handles DynamoDB JSON data processing from S3 via Lambda triggers.
It provides functionality for reading, transforming, and writing lead data
to Hudi tables with proper error handling and logging.
"""

import traceback
import json
from typing import Dict, List, Any, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.functions import col, concat_ws, collect_list
import sys
import logging
import datetime

sys.path.append('/home/hadoop')
import boto3
from boto3.dynamodb.types import TypeDeserializer
import pandas as pd
import awswrangler as wr


# =============================================================================
# Custom Exception Classes
# =============================================================================

class NoeLeadError(Exception):
    """Base exception class for NeoLead processing errors."""
    pass


class ConfigError(NoeLeadError):
    """Exception raised for configuration-related errors."""
    pass


class FileReadError(NoeLeadError):
    """Exception raised when reading files fails."""
    pass


class FileWriteError(NoeLeadError):
    """Exception raised when writing files fails."""
    pass


class DataTransformationError(NoeLeadError):
    """Exception raised during data transformation operations."""
    pass


class SparkSessionError(NoeLeadError):
    """Exception raised when Spark session creation fails."""
    pass


# =============================================================================
# Logging Setup
# =============================================================================

log_file_name = 'job_' + str(datetime.datetime.now().strftime('%Y%m%d_%H%M%S_%f')) + '.log'
extra = {'log_file_name': log_file_name}
logger = logging.getLogger(__name__)
syslog = logging.FileHandler(log_file_name, mode='w')
formatter = logging.Formatter('%(log_file_name)s;%(asctime)s;%(levelname)s;%(message)s')
syslog.setFormatter(formatter)
logger.setLevel(logging.INFO)
logger.addHandler(syslog)
logger = logging.LoggerAdapter(logger, extra)


# =============================================================================
# DynamoDB JSON Handler Class
# =============================================================================

class DynamoDBJsonHandler:
    """
    Handles DynamoDB JSON deserialization and flattening operations.

    This class provides methods to convert DynamoDB's native JSON format
    to standard Python dictionaries and flatten nested structures.

    Attributes:
        deserializer: TypeDeserializer instance for DynamoDB JSON conversion
    """

    def __init__(self) -> None:
        """Initialize the DynamoDB JSON handler with a TypeDeserializer."""
        self.deserializer = TypeDeserializer()

    def deserialize_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert a DynamoDB JSON item to a standard Python dictionary.

        Args:
            item: DynamoDB JSON formatted dictionary with type descriptors

        Returns:
            Standard Python dictionary with deserialized values

        Raises:
            DataTransformationError: If deserialization fails
        """
        try:
            return {k: self.deserializer.deserialize(value=v) for k, v in item.items()}
        except Exception as e:
            raise DataTransformationError(f"Failed to deserialize DynamoDB item: {e}")

    def flatten_nested_structure(
        self,
        data: Any,
        prefix: str = ''
    ) -> List[Tuple[str, Any]]:
        """
        Recursively flatten a nested dictionary/list structure.

        Args:
            data: The data structure to flatten (dict, list, or primitive)
            prefix: Current path prefix for nested keys

        Returns:
            List of tuples containing (flattened_key, value) pairs
        """
        result = []

        def _flatten(value: Any, current_prefix: str) -> None:
            if isinstance(value, dict):
                for k, v in value.items():
                    new_prefix = f"{current_prefix}.{k}" if current_prefix else k
                    _flatten(v, new_prefix)
            elif isinstance(value, list):
                for i, v in enumerate(value):
                    new_prefix = f"{current_prefix}.{i}"
                    _flatten(v, new_prefix)
            else:
                result.append((current_prefix, value))

        _flatten(data, prefix)
        return result

    def normalize_column_names(self, columns: List[str]) -> List[str]:
        """
        Normalize column names by replacing dots with underscores.

        Args:
            columns: List of column names to normalize

        Returns:
            List of normalized column names
        """
        return [col.replace('.', '_') for col in columns]


# Legacy function wrapper for backward compatibility
def from_dynamodb_to_json(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert DynamoDB JSON item to standard Python dictionary.

    This is a wrapper function for backward compatibility.

    Args:
        item: DynamoDB JSON formatted dictionary

    Returns:
        Standard Python dictionary with deserialized values
    """
    handler = DynamoDBJsonHandler()
    return handler.deserialize_item(item)


def flatten_nested_json(data: Any, prefix: str = '') -> List[Tuple[str, Any]]:
    """
    Flatten nested JSON structure into key-value pairs.

    Args:
        data: Nested data structure to flatten
        prefix: Optional prefix for keys

    Returns:
        List of (key, value) tuples
    """
    handler = DynamoDBJsonHandler()
    return handler.flatten_nested_structure(data, prefix)


# =============================================================================
# Configuration Handling
# =============================================================================

REQUIRED_CONFIG_KEYS = [
    'load_type',
    'source_path',
    'destination_path',
    'table_name',
    'source_system',
    'log_file_path',
    'spark_properties',
    'hudi_properties',
    'file_output_format',
    'write_mode'
]

INCREMENTAL_REQUIRED_KEYS = [
    'unprocessed_file_path',
    'processed_file_path',
    'number_of_files_to_read'
]


def validate_config(config: Dict[str, Any]) -> None:
    """
    Validate that all required configuration parameters are present.

    Args:
        config: Configuration dictionary to validate

    Raises:
        ConfigError: If required parameters are missing or invalid
    """
    missing_keys = [key for key in REQUIRED_CONFIG_KEYS if key not in config]
    if missing_keys:
        raise ConfigError(f"Missing required config parameters: {missing_keys}")

    if config.get('load_type', '').lower() == 'incremental':
        missing_incremental = [
            key for key in INCREMENTAL_REQUIRED_KEYS if key not in config
        ]
        if missing_incremental:
            raise ConfigError(
                f"Missing required config parameters for incremental load: {missing_incremental}"
            )

    if not config.get('source_path'):
        raise ConfigError("source_path cannot be empty")

    if not config.get('destination_path'):
        raise ConfigError("destination_path cannot be empty")


def read_config(config_path: str) -> Dict[str, Any]:
    """
    Read and parse configuration from file path (S3 or local filesystem).

    Args:
        config_path: Path to the JSON configuration file (S3 or local)

    Returns:
        Parsed configuration dictionary

    Raises:
        ConfigError: If configuration file cannot be read or parsed
    """
    logger.info("Inside read config")
    try:
        if config_path.startswith('s3'):
            logger.info("Reading config file from S3")
            s3 = boto3.resource('s3')
            bucket_name = config_path.split('/')[2]
            key = '/'.join(config_path.split('/')[3:])
            file_object = s3.Object(bucket_name, key)
            file_content = file_object.get()['Body'].read().decode('utf-8')
            json_content = json.loads(file_content)
        else:
            logger.info(f"Reading config file from path: {config_path}")
            with open(config_path, 'r') as config_file:
                json_content = json.load(config_file)

        logger.info(f"Input Config Details: {json.dumps(json_content)}")

        validate_config(json_content)

        return json_content
    except ConfigError:
        raise
    except FileNotFoundError:
        raise ConfigError(f"Config file not found: {config_path}")
    except json.JSONDecodeError as e:
        raise ConfigError(f"Invalid JSON in config file: {e}")
    except Exception as e:
        raise ConfigError(f"Error reading config file: {e}")


# =============================================================================
# Record Count and Statistics
# =============================================================================

def get_record_count(
    spark: SparkSession,
    initial_df: SparkDataFrame,
    config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Calculate record counts and sequence number statistics from the dataframe.

    Args:
        spark: Active SparkSession
        initial_df: Input dataframe to analyze
        config: Configuration dictionary

    Returns:
        Dictionary containing insert/update/delete counts and sequence numbers

    Raises:
        DataTransformationError: If count calculation fails
    """
    try:
        logger.info("Getting count of insert, update and delete records")
        record_type_counts = {'I': 0, 'U': 0, 'D': 0, 'SQ': '0', 'EQ': '0'}

        logger.info("Getting counts of record types.")
        initial_df.createOrReplaceTempView("rec_count")

        det_sql = "SELECT min(sequence_no) as SQ, max(sequence_no) as EQ FROM rec_count"
        det_list = spark.sql(det_sql).toJSON().collect()

        if det_list:
            row_dict = json.loads(det_list[0])
            record_type_counts['SQ'] = str(row_dict.get('SQ', '0') or '0')
            record_type_counts['EQ'] = str(row_dict.get('EQ', '0') or '0')

        record_type_counts['I'] = initial_df.count()

        load_type = config.get('load_type', '').lower()
        logger.info(f"Getting count for {load_type} load")

        return record_type_counts
    except Exception as e:
        raise DataTransformationError(f"Error getting record count: {e}")


# =============================================================================
# File Write Operations
# =============================================================================

def write_file(
    df: SparkDataFrame,
    config: Dict[str, Any],
    write_type: str
) -> None:
    """
    Write dataframe to destination path in the specified format.

    Args:
        df: Spark dataframe to write
        config: Configuration dictionary with destination paths and format settings
        write_type: Type of write operation ('error' for error records, '' for normal)

    Raises:
        FileWriteError: If writing to destination fails
    """
    logger.info("Inside write file")
    print("Inside write file")
    try:
        if write_type == 'error':
            error_file_path = config['error_records_destination_path']
            logger.info(f"Writing error records to path: {error_file_path}")
            df.coalesce(1).write \
                .partitionBy("source_system", "table_name", "year", "month", "day") \
                .format('parquet') \
                .mode('append') \
                .save(error_file_path)
        else:
            output_file_path = f"{config['destination_path']}/{config['table_name']}"
            logger.info(f"Writing output file to path: {output_file_path}")
            print(f"Writing output file to path: {output_file_path}")
            df.write \
                .format(config['file_output_format']) \
                .options(**config['hudi_properties']) \
                .mode(config['write_mode']) \
                .save(output_file_path)

        logger.info("Output file written successfully")
        print("Output file written successfully")
    except Exception as e:
        raise FileWriteError(f"Error writing output file: {e}")


def upload_log_file(config: Dict[str, Any]) -> None:
    """
    Upload the job log file to S3 bucket.

    Args:
        config: Configuration dictionary with log file path settings

    Raises:
        FileWriteError: If log file upload fails
    """
    logger.info("Inside upload file")
    try:
        s3_client = boto3.client('s3')
        bucket_name = config['log_file_path'].split('/')[2]
        file_name = '/'.join(config['log_file_path'].split('/')[3:])
        filepath = (
            f"{file_name}/{config['source_system'].lower()}/"
            f"{config['table_name']}/{log_file_name}"
        )
        logger.info("Log File Uploaded")
        response = s3_client.upload_file(log_file_name, bucket_name, filepath)
        print(f"Log File Upload Response: {response}")
    except Exception as e:
        print(f"Error uploading log file: {e}")


# =============================================================================
# File List Operations
# =============================================================================


def get_unprocessed_folder_file_list(config: Dict[str, Any]) -> List[str]:
    """
    Get list of CSV files from the unprocessed folder in S3.

    Args:
        config: Configuration dictionary with unprocessed file path

    Returns:
        List of S3 file paths to process

    Raises:
        FileReadError: If reading file list fails
    """
    logger.info("Inside unprocessed folder file list")
    print("Inside unprocessed folder file list")
    try:
        bucket_name = config['unprocessed_file_path'].split('/')[2]
        prefix = '/'.join(config['unprocessed_file_path'].split('/')[3:]) + '/'
        logger.info(f"path for incremental bookmarks: {prefix}")
        print(f"path for incremental bookmarks: {prefix}")

        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_name)
        file_list = []

        for obj in bucket.objects.filter(Prefix=prefix):
            if '.csv' in obj.key:
                file_list.append(f"s3://{bucket_name}/{obj.key}")

        if not file_list:
            logger.info("No new files to process")
            return file_list

        logger.info(f"Files read from unprocessed folder in this run: {file_list}")

        num_files = config.get('number_of_files_to_read', '')
        if num_files == '':
            logger.info("All files in unprocessed folder will be processed in this run.")
            print("All files in unprocessed folder will be processed in this run.")
            return file_list
        else:
            logger.info(f"{num_files} files will be processed in this run.")
            print(f"{num_files} files will be processed in this run.")
            return file_list[0:int(num_files)]
    except Exception as e:
        raise FileReadError(f"Error reading unprocessed files: {e}")


def get_unprocessed_file_list(
    spark: SparkSession,
    unprocessed_list: List[str]
) -> List[str]:
    """
    Extract raw file paths from unprocessed bookmark CSV files.

    Args:
        spark: Active SparkSession
        unprocessed_list: List of CSV bookmark file paths

    Returns:
        List of raw S3 file paths to process

    Raises:
        FileReadError: If reading bookmark files fails
    """
    logger.info("Inside get unprocessed file list")
    print("Inside get unprocessed file list")
    try:
        unprocessed_folder_df = spark.read.option("header", "true") \
            .option("inferSchema", "true") \
            .format("csv") \
            .load(unprocessed_list) \
            .withColumn("filePath", concat_ws('/', col("bucketName"), col("fileName")))

        file_paths = unprocessed_folder_df.agg(
            collect_list(col("filePath"))
        ).collect()[0][0]

        file_list = [f's3://{x}' for x in file_paths]
        return file_list
    except Exception as e:
        raise FileReadError(f"Error getting list of unprocessed files: {e}")


# =============================================================================
# File Movement Operations
# =============================================================================


def move_unprocessed_to_processed(
    config: Dict[str, Any],
    file_list: List[str]
) -> None:
    """
    Move files from unprocessed folder to processed folder in S3.

    Args:
        config: Configuration dictionary with processed/unprocessed paths
        file_list: List of file paths to move

    Raises:
        FileWriteError: If file movement fails
    """
    logger.info("Inside unprocessed to processed movement")
    try:
        bucket_name = config['processed_file_path'].split('/')[2]
        new_prefix = '/'.join(config['processed_file_path'].split('/')[3:])
        old_prefix = '/'.join(config['unprocessed_file_path'].split('/')[3:])

        logger.info(f"new_prefix: {new_prefix}")
        print(f"new_prefix: {new_prefix}")
        logger.info(f"old_prefix: {old_prefix}")
        print(f"old_prefix: {old_prefix}")

        s3_resource = boto3.resource('s3')

        for file in file_list:
            file_name = file.split('/')[-1]
            old_path = f"{bucket_name}/{old_prefix}/{file_name}"
            new_path = f"{new_prefix}/{file_name}"
            s3_resource.Object(bucket_name, new_path).copy_from(CopySource=old_path)
            s3_resource.Object(bucket_name, f"{old_prefix}/{file_name}").delete()
    except Exception as e:
        raise FileWriteError(f"Error moving files from unprocessed to processed folder: {e}")


# =============================================================================
# Audit and Spark Session
# =============================================================================


def audit_table_entry(
    spark: SparkSession,
    config: Dict[str, Any],
    status: str,
    record_count_dict: Dict[str, Any],
    epoch_ts: int,
    start_ts: str
) -> None:
    """
    Add a new entry in the audit table for job tracking.

    Args:
        spark: Active SparkSession
        config: Configuration dictionary
        status: Job status ('Started', 'Completed', 'Failed')
        record_count_dict: Dictionary with I/U/D counts and sequence numbers
        epoch_ts: Epoch timestamp of job start
        start_ts: Formatted timestamp of job start

    Raises:
        DataTransformationError: If audit table update fails
    """
    try:
        audit_table_columns = [
            'epoch_timestamp', 'source_system', 'table_name', 'status',
            'insert_count', 'update_count', 'delete_count',
            'record_start_sequence', 'record_end_sequence', 'log_file_name'
        ]

        data = [(
            epoch_ts,
            config['source_system'],
            config['table_name'],
            status,
            record_count_dict['I'],
            record_count_dict['U'],
            record_count_dict['D'],
            record_count_dict['SQ'],
            record_count_dict['EQ'],
            log_file_name
        )]

        audit_table_df = spark.createDataFrame(data).toDF(*audit_table_columns)

        audit_hudi_properties = {
            "hoodie.table.name": "ETL_CURATED_LOG",
            "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
            "hoodie.datasource.write.recordkey.field": "epoch_timestamp, source_system, table_name",
            "hoodie.datasource.write.partitionpath.field": "",
            "hoodie.datasource.write.precombine.field": "record_end_sequence",
            "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.CustomKeyGenerator",
            "hoodie.datasource.write.operation": "upsert",
            "hoodie.parquet.compression.codec": "snappy",
            "hoodie.cleaner.policy": "KEEP_LATEST_COMMITS",
            "hoodie.keep.min.commits": 2,
            "hoodie.keep.max.commits": 3,
            "hoodie.cleaner.commits.retained": 1,
            "hoodie.clean.automatic": "True"
        }

        audit_table_path = (
            f"{config['audit_table_path']}/"
            f"{config['source_system'].lower()}/"
            f"{config['table_name'].lower()}"
        )

        audit_table_df.coalesce(1).write \
            .format('hudi') \
            .options(**audit_hudi_properties) \
            .mode('append') \
            .save(audit_table_path)

        logger.info('Audit table updated successfully.')
    except Exception as e:
        raise DataTransformationError(f"Error updating audit table: {e}")


def create_spark_session(config: Dict[str, Any]) -> SparkSession:
    """
    Create and configure a SparkSession based on config settings.

    Args:
        config: Configuration dictionary with spark_properties

    Returns:
        Configured SparkSession instance

    Raises:
        SparkSessionError: If session creation fails
    """
    logger.info("Inside create spark session")
    try:
        conf = SparkConf()
        conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
        conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
        conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
        conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

        spark_conf = config.get('spark_properties', {})
        for key, value in spark_conf.items():
            conf.set(key, value)

        app_name = config.get('application_name', '') or 'DefaultApp'

        spark = SparkSession \
            .builder \
            .config(conf=conf) \
            .appName(app_name) \
            .enableHiveSupport() \
            .getOrCreate()

        logger.info("Spark session object created")
        return spark
    except Exception as e:
        raise SparkSessionError(f"Error in Spark Session Creation: {e}")


# =============================================================================
# Data Transformation Functions
# =============================================================================


def normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names in a pandas DataFrame by replacing dots with underscores.

    Args:
        df: pandas DataFrame with columns to normalize

    Returns:
        DataFrame with normalized column names
    """
    handler = DynamoDBJsonHandler()
    df.columns = handler.normalize_column_names(df.columns.tolist())
    return df


def flatten_nested_records(
    df: pd.DataFrame,
    nested_fields: List[str],
    join_key: str = 'leadId'
) -> pd.DataFrame:
    """
    Flatten nested list fields in a DataFrame and merge back.

    Args:
        df: pandas DataFrame with nested fields
        nested_fields: List of field paths to flatten (e.g., ['products', 'personalDetails.partyList'])
        join_key: Column to use for merging flattened data

    Returns:
        DataFrame with flattened nested fields
    """
    result_df = df.copy()
    tmp = df.to_dict('records')

    for field in nested_fields:
        prefix = f"{field}."
        try:
            normalized = pd.json_normalize(
                tmp,
                [field],
                record_prefix=prefix,
                errors='ignore',
                meta=[join_key]
            )
            if not normalized.empty:
                result_df = result_df.merge(
                    normalized,
                    on=[join_key],
                    how='outer'
                )
                if field in result_df.columns:
                    result_df = result_df.drop(field, axis=1)
        except (KeyError, TypeError):
            continue

    return result_df


def read_file(
    spark: SparkSession,
    config: Dict[str, Any],
    file: Optional[str],
    file_list_to_process: List[str]
) -> pd.DataFrame:
    """
    Read and process input files based on load type (full or incremental).

    Args:
        spark: Active SparkSession
        config: Configuration dictionary
        file: Single file path for full load
        file_list_to_process: List of file paths for incremental load

    Returns:
        Processed pandas DataFrame with flattened nested structures

    Raises:
        FileReadError: If reading input files fails
    """
    logger.info("Inside read file")
    print("Inside read file")

    dynamodb_handler = DynamoDBJsonHandler()

    try:
        load_type = config.get('load_type', '').lower()

        if load_type == 'full':
            return _process_full_load(file, dynamodb_handler)
        else:
            return _process_incremental_load(file_list_to_process, dynamodb_handler)
    except Exception as e:
        raise FileReadError(f"Error reading input file: {e}")


def _process_full_load(
    file_path: str,
    handler: DynamoDBJsonHandler
) -> pd.DataFrame:
    """
    Process files for full load operation.

    Args:
        file_path: Path to the parquet file
        handler: DynamoDBJsonHandler instance

    Returns:
        Processed pandas DataFrame
    """
    logger.info(f"Reading input file from path: {file_path}")
    print(f"Reading input file from path: {file_path}")

    df = wr.s3.read_parquet(file_path)
    df = df.reset_index(drop=True)

    lst_dict = [json.loads(df.loc[i].to_json()) for i in range(len(df))]
    normalized_df = pd.json_normalize(lst_dict)

    if 'sequence' in normalized_df.columns:
        normalized_df = normalized_df.drop(['sequence'], axis=1)

    nested_fields = ['products', 'personalDetails.partyList']
    result_df = flatten_nested_records(normalized_df, nested_fields, 'leadId')

    result_df = normalize_dataframe_columns(result_df)
    result_df = result_df.fillna('')

    return result_df


def _process_incremental_load(
    file_list: List[str],
    handler: DynamoDBJsonHandler
) -> pd.DataFrame:
    """
    Process files for incremental load operation.

    Args:
        file_list: List of JSON file paths to process
        handler: DynamoDBJsonHandler instance

    Returns:
        Processed pandas DataFrame with sequence numbers
    """
    logger.info("Reading input files from paths")
    print("Reading input files from paths")

    lst_dict = []
    seq_num = []

    for file_path in file_list:
        df = wr.s3.read_json(file_path, use_threads=True)
        records = df['Records'].tolist()

        for record in records:
            new_image = record['dynamodb']['NewImage']
            deserialized = handler.deserialize_item(new_image)
            lst_dict.append(deserialized)
            seq_num.append(record['dynamodb']['SequenceNumber'])

    logger.info("Out of the Loop")
    print("Out of the Loop")

    normalized_df = pd.json_normalize(lst_dict)

    if 'sequence' in normalized_df.columns:
        normalized_df = normalized_df.drop(['sequence'], axis=1)

    normalized_df['Sequence_No'] = seq_num

    logger.info("Normalized DataFrame is created")
    print("Normalized DataFrame is created")

    nested_fields = ['products', 'personalDetails.partyList']
    result_df = flatten_nested_records(normalized_df, nested_fields, 'leadId')

    result_df = result_df.fillna('')
    result_df = normalize_dataframe_columns(result_df)

    logger.info("Columns renaming is done")
    print("Columns renaming is done")

    return result_df


def check_and_add_missing_columns(
    config: Dict[str, Any],
    spark: SparkSession,
    initial_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Check for missing columns compared to existing data and add them.

    Args:
        config: Configuration dictionary
        spark: Active SparkSession
        initial_df: Input pandas DataFrame

    Returns:
        DataFrame with missing columns added

    Raises:
        DataTransformationError: If column check fails
    """
    try:
        output_file_path = f"{config['destination_path']}/{config['table_name']}/*.parquet"
        logger.info(f"path to read for checking missing columns in raw data: {output_file_path}")
        print(f"path to read for checking missing columns in raw data: {output_file_path}")

        existing_columns = set()
        chk_df = wr.s3.read_parquet(output_file_path, chunked=True)

        for df_chunk in chk_df:
            columns = df_chunk.columns.to_list()[5:]
            existing_columns.update(columns)

        current_columns = set(initial_df.columns.to_list())
        missing_columns = existing_columns - current_columns

        for column_name in missing_columns:
            initial_df[column_name] = ''

        return initial_df
    except Exception as e:
        raise DataTransformationError(f"Error checking columns: {e}")


def get_full_load_file_list(config: Dict[str, Any]) -> List[str]:
    """
    Get list of parquet files for full load from S3.

    Args:
        config: Configuration dictionary with source_path

    Returns:
        List of S3 file paths to process

    Raises:
        FileReadError: If reading file list fails
    """
    try:
        s3 = boto3.resource('s3')
        logger.info('Getting bucket name from source path')
        print('Getting bucket name from source path')

        bucket_name = config['source_path'].split('/')[2]
        bucket = s3.Bucket(bucket_name)

        logger.info(f"Bucket: {bucket}")
        print(f"Bucket: {bucket}")

        prefix = '/'.join(config['source_path'].split('/')[3:]) + '/'

        logger.info(f"Prefix: {prefix}")
        print(f"Prefix: {prefix}")

        file_list = []
        for obj in bucket.objects.filter(Prefix=prefix, Delimiter='/'):
            if '.parquet' in obj.key:
                file_list.append(f"s3://{bucket_name}/{obj.key}")

        logger.info(f"Files found: {file_list}")
        print(f"Files found: {file_list}")

        return file_list
    except Exception as e:
        raise FileReadError(f"Error reading full load file list: {e}")


# =============================================================================
# Main Entry Point
# =============================================================================

# Batch size for incremental processing
INCREMENTAL_BATCH_SIZE = 10000


def main() -> None:
    """
    Main entry point for the NeoLead data processing job.

    Reads configuration, creates Spark session, processes data files
    based on load type (full or incremental), and writes output.

    Exit codes:
        0: Success
        1: Failure or incorrect arguments
    """
    logger.info("Inside main function")

    if len(sys.argv) != 2:
        logger.info(f"Argument count: {len(sys.argv)}")
        logger.info(f"Command line arguments: {sys.argv}")
        logger.info("Incorrect command line arguments. Expected: python noe_lead.py <config_path>")
        exit(1)

    config: Dict[str, Any] = {}
    file_list_to_process: List[str] = []
    unprocessed_list: List[str] = []
    job_status = ''

    try:
        logger.info("Calling function to read config file")
        config = read_config(sys.argv[1])

        logger.info("Calling function to create Spark session object")
        spark = create_spark_session(config)

        load_type = config.get('load_type', '').lower()

        if load_type == 'incremental':
            logger.info("Calling function to get list of files in unprocessed folder.")
            print("Calling function to get list of files in unprocessed folder.")
            unprocessed_list = get_unprocessed_folder_file_list(config)
            if unprocessed_list:
                file_list_to_process = get_unprocessed_file_list(spark, unprocessed_list)

        should_process = (
            (load_type == 'incremental' and file_list_to_process)
            or load_type == 'full'
        )

        if should_process:
            logger.info("Calling function to read input file")
            print("Calling function to read input file")

            if load_type == 'full':
                _process_full_load_files(spark, config, file_list_to_process)
            elif load_type == 'incremental' and file_list_to_process:
                _process_incremental_load_files(
                    spark, config, file_list_to_process, unprocessed_list
                )
        else:
            logger.info("No files to process.")

        logger.info("Job Completed successfully")
        print("Job Completed successfully")
        logger.info("Calling function to update job end entry with successful status")
        print("Calling function to update job end entry with successful status")
        job_status = 'Success'

    except NoeLeadError as e:
        job_status = 'Failed'
        logger.error(f"NeoLead Error: {e}")
        logger.error(traceback.format_exc().replace('\n', '|'))
        print("Calling function to update job end entry with failed status")
    except Exception as e:
        job_status = 'Failed'
        logger.error(f"Unexpected error: {e}")
        logger.error(traceback.format_exc().replace('\n', '|'))
        logger.error("Calling function to update job end entry with failed status")
        print("Calling function to update job end entry with failed status")

    finally:
        if config:
            upload_log_file(config)
        if job_status == 'Failed':
            exit(1)
        else:
            exit(0)


def _process_full_load_files(
    spark: SparkSession,
    config: Dict[str, Any],
    file_list_to_process: List[str]
) -> None:
    """
    Process files for full load operation.

    Args:
        spark: Active SparkSession
        config: Configuration dictionary
        file_list_to_process: List of files (unused for full load)
    """
    files_read = get_full_load_file_list(config)

    for file_path in files_read:
        initial_df = read_file(spark, config, file_path, file_list_to_process)
        spark_df = spark.createDataFrame(initial_df.astype(str))
        spark_df = spark_df.withColumn(
            'Sequence_No',
            F.lit(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
        )

        logger.info("Writing output dataframe")
        print("Writing output dataframe")
        write_file(spark_df, config, '')


def _process_incremental_load_files(
    spark: SparkSession,
    config: Dict[str, Any],
    file_list_to_process: List[str],
    unprocessed_list: List[str]
) -> None:
    """
    Process files for incremental load operation in batches.

    Args:
        spark: Active SparkSession
        config: Configuration dictionary
        file_list_to_process: List of raw files to process
        unprocessed_list: List of bookmark files to move after processing
    """
    for i in range(0, len(file_list_to_process), INCREMENTAL_BATCH_SIZE):
        file_batch = file_list_to_process[i:i + INCREMENTAL_BATCH_SIZE]

        initial_df = read_file(spark, config, None, file_batch)
        initial_df = check_and_add_missing_columns(config, spark, initial_df)
        spark_df = spark.createDataFrame(initial_df.astype(str))

        logger.info("Writing output dataframe")
        print("Writing output dataframe")
        write_file(spark_df, config, '')

        bookmark_batch = unprocessed_list[i:i + INCREMENTAL_BATCH_SIZE]
        move_unprocessed_to_processed(config, bookmark_batch)


if __name__ == "__main__":
    # calling main function
    logger.info("Calling main function")
    main()
