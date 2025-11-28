import traceback
import json
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.functions import col, concat_ws, when, collect_list, lit, to_timestamp
from pyspark.sql.functions import year, month, date_format
import sys
import logging
import datetime
import time

sys.path.append('/home/hadoop')
import boto3
from boto3.dynamodb.types import TypeDeserializer
import pandas as pd
import awswrangler as wr

# Creating log file name
log_file_name = 'job_' + str(datetime.datetime.now().strftime('%Y%m%d_%H%M%S_%f')) + '.log'
extra = {'log_file_name': log_file_name}
logger = logging.getLogger(__name__)
syslog = logging.FileHandler(log_file_name, mode='w')
formatter = logging.Formatter('%(log_file_name)s;%(asctime)s;%(levelname)s;%(message)s')
syslog.setFormatter(formatter)
logger.setLevel(logging.INFO)
logger.addHandler(syslog)
logger = logging.LoggerAdapter(logger, extra)


def from_dynamodb_to_json(item):
    """
    Convert DynamoDB JSON format to standard JSON using TypeDeserializer.
    Handles nested structures and ensures complete deserialization.
    
    :param item: DynamoDB JSON item
    :return: Standard JSON dictionary
    """
    if item is None:
        return {}
    
    d = TypeDeserializer()
    result = {}
    
    for k, v in item.items():
        try:
            deserialized_value = d.deserialize(value=v)
            # Convert Decimal to float/int for JSON compatibility
            if isinstance(deserialized_value, Decimal):
                if deserialized_value % 1 == 0:
                    deserialized_value = int(deserialized_value)
                else:
                    deserialized_value = float(deserialized_value)
            result[k] = deserialized_value
        except Exception as e:
            logger.warning(f"Failed to deserialize key '{k}': {str(e)}")
            result[k] = None
    
    return result


def validate_dataframe(df, stage_name, key_column='leadId'):
    """
    Validate dataframe at a transformation checkpoint.
    Logs row count and sample records for debugging.
    
    :param df: Pandas DataFrame to validate
    :param stage_name: Name of the transformation stage
    :param key_column: Primary key column to check for nulls
    :return: Tuple of (row_count, null_key_count)
    """
    row_count = len(df)
    null_key_count = 0
    
    if key_column in df.columns:
        null_key_count = df[key_column].isna().sum() + (df[key_column] == '').sum()
    
    logger.info(f"[{stage_name}] Row count: {row_count}, Null/empty {key_column}: {null_key_count}")
    
    # Log sample records for debugging (first 3 records)
    if row_count > 0 and row_count <= 3:
        for idx in range(min(3, row_count)):
            sample = df.iloc[idx].to_dict()
            logger.info(f"[{stage_name}] Sample record {idx}: {str(sample)[:500]}")
    
    return row_count, null_key_count


def safe_normalize_nested(df_records, nested_path, record_prefix, meta_fields):
    """
    Safely normalize nested JSON arrays with proper error handling.
    Preserves records even when nested arrays are empty or missing.
    
    :param df_records: List of dictionaries (records)
    :param nested_path: Path to nested array (e.g., ['products'])
    :param record_prefix: Prefix for flattened column names
    :param meta_fields: List of meta fields to include (e.g., ['leadId'])
    :return: Normalized pandas DataFrame
    """
    try:
        # Filter records that have the nested path
        records_with_nested = []
        for record in df_records:
            nested_value = record
            path_parts = nested_path if isinstance(nested_path, list) else [nested_path]
            
            # Navigate to nested value
            for part in path_parts:
                if isinstance(nested_value, dict) and part in nested_value:
                    nested_value = nested_value[part]
                else:
                    nested_value = None
                    break
            
            # Only include if nested value exists and is a non-empty list
            if isinstance(nested_value, list) and len(nested_value) > 0:
                records_with_nested.append(record)
        
        if not records_with_nested:
            logger.info(f"No records found with nested path {nested_path}")
            # Return empty dataframe with meta columns
            return pd.DataFrame(columns=[record_prefix + col for col in []] + meta_fields)
        
        # Normalize the filtered records
        normalized_df = pd.json_normalize(
            records_with_nested,
            nested_path,
            record_prefix=record_prefix,
            errors='ignore',
            meta=meta_fields
        )
        
        logger.info(f"Normalized {len(normalized_df)} rows from nested path {nested_path}")
        return normalized_df
        
    except Exception as e:
        logger.warning(f"Error normalizing nested path {nested_path}: {str(e)}")
        return pd.DataFrame(columns=meta_fields)


def safe_merge(left_df, right_df, on_column, how='left'):
    """
    Safely merge two dataframes with validation to prevent data loss.
    Uses left join by default to preserve all records from the main dataframe.
    
    :param left_df: Left DataFrame (main data)
    :param right_df: Right DataFrame (normalized nested data)
    :param on_column: Column to merge on
    :param how: Type of merge (default 'left' to preserve main data)
    :return: Merged DataFrame
    """
    left_count = len(left_df)
    right_count = len(right_df)
    
    logger.info(f"Merging: left_df rows={left_count}, right_df rows={right_count}, on='{on_column}', how='{how}'")
    
    # Check if merge column exists in both dataframes
    if on_column not in left_df.columns:
        logger.error(f"Merge column '{on_column}' not found in left DataFrame")
        return left_df
    
    if right_count == 0 or on_column not in right_df.columns:
        logger.info(f"Right DataFrame is empty or missing merge column, returning left DataFrame")
        return left_df
    
    # Perform merge
    merged_df = left_df.merge(right_df, on=on_column, how=how)
    merged_count = len(merged_df)
    
    logger.info(f"Merge result: {merged_count} rows")
    
    # Validate no data loss for left join
    if how == 'left' and merged_count < left_count:
        logger.warning(f"Data loss detected during merge! Before: {left_count}, After: {merged_count}")
    
    return merged_df


def normalize_column_names(df):
    """
    Normalize column names by replacing dots with underscores.
    
    :param df: Pandas DataFrame
    :return: DataFrame with normalized column names
    """
    new_columns = []
    for col in df.columns:
        if '.' in col:
            new_col = col.replace('.', '_')
            new_columns.append(new_col)
        else:
            new_columns.append(col)
    
    df.columns = new_columns
    return df


def read_config(config_path):
    """
    Read configuration from S3 or local file system.
    
    :param config_path: Path to JSON config file
    :return: Configuration dictionary
    """
    logger.info("Inside read config")
    try:
        if config_path[0:2] == 's3':
            logger.info("Reading config file from S3")
            s3 = boto3.resource('s3')
            file_object = s3.Object(config_path.split('/')[2], '/'.join(config_path.split('/')[3:]))
            file_content = file_object.get()['Body'].read().decode('utf-8')
            json_content = json.loads(file_content)
            json_object = json.dumps(json_content)
        else:
            logger.info("Reading config file from path : " + config_path)
            json_content = json.load(open(config_path, 'r'))
            json_object = json.dumps(json_content)
            logger.info("Input Config Details:")
        
        logger.info(json_object)
        return json_content
    except Exception as e:
        logger.error(f"Error reading config: {str(e)}")
        raise Exception("Error reading config.")


def get_record_count(spark, initial_df, config):
    """
    Get record counts including min/max sequence numbers.
    
    :param spark: Spark session object
    :param initial_df: Initial DataFrame
    :param config: Configuration dictionary
    :return: Dictionary with counts and sequence numbers
    """
    try:
        logger.info("Getting count of insert, update and delete records")
        record_type_counts = {'I': 0, 'U': 0, 'D': 0, 'SQ': '0', 'EQ': '0'}

        logger.info("Getting counts of record types.")
        det_sql = "select min(sequence_no) as SQ, max(sequence_no) as EQ from rec_count"

        initial_df.createOrReplaceTempView("rec_count")

        det_list = spark.sql(det_sql).toJSON().collect()
        row_dict = json.loads(det_list[0])
        
        for key in row_dict.keys():
            if key == 'SQ':
                if row_dict['SQ'] in ['', ' ', None]:
                    record_type_counts[key] = '0'
                else:
                    record_type_counts[key] = str(row_dict['SQ'])
            elif key == 'EQ':
                if row_dict['EQ'] in ['', ' ', None]:
                    record_type_counts[key] = '0'
                else:
                    record_type_counts[key] = str(row_dict['EQ'])
        
        if config['load_type'].lower() == 'full':
            logger.info("Getting count for full load")
            record_type_counts['I'] = initial_df.count()
        else:
            logger.info("Getting count for incremental load")
            record_type_counts['I'] = initial_df.count()

        return record_type_counts
    except Exception as e:
        logger.error(f"Error getting record count: {str(e)}")
        raise Exception("Error getting record count.")


def write_file(df, config, write_type):
    """
    Write DataFrame to S3 in Parquet format using Hudi.
    
    :param df: Spark DataFrame to write
    :param config: Configuration dictionary
    :param write_type: Type of write ('error' or '')
    """
    logger.info("Inside write file")
    print("Inside write file")
    try:
        if write_type == 'error':
            error_file_path = config['error_records_destination_path']
            logger.info("Writing error records to path : " + error_file_path)
            df.coalesce(1).write \
                .partitionBy("source_system", "table_name", "year", "month", "day") \
                .format('parquet') \
                .mode('append') \
                .save(error_file_path)
        else:
            output_file_path = config['destination_path'] + '/' + config['table_name']
            logger.info("Writing output file to path : " + output_file_path)
            print("Writing output file to path : " + output_file_path)
            df.write \
                .format(config['file_output_format']) \
                .options(**config['hudi_properties']) \
                .mode(config['write_mode']) \
                .save(output_file_path)
        
        logger.info("Output file written successfully")
        print("Output file written successfully")
    except Exception as e:
        logger.error(f"Error writing output file: {str(e)}")
        raise Exception("Error writing output file.")


def upload_log_file(config):
    """
    Upload log file to S3 bucket.
    
    :param config: Configuration dictionary
    """
    logger.info("Inside upload file")
    s3_client = boto3.client('s3')
    bucket_name = config['log_file_path'].split('/')[2]
    file_name = '/'.join(config['log_file_path'].split('/')[3:])
    filepath = file_name + '/' + config['source_system'].lower() + '/' + config['table_name'] + '/' + log_file_name
    logger.info("Log File Uploaded")
    response = s3_client.upload_file(log_file_name, bucket_name, filepath)
    print("Log File Upload Response : ", response)


def get_unprocessed_folder_file_list(config):
    """
    Get list of files in the unprocessed folder.
    
    :param config: Configuration dictionary
    :return: List of file paths
    """
    logger.info("Inside unprocessed folder file list")
    print("Inside unprocessed folder file list")
    try:
        bucket_name = config['unprocessed_file_path'].split('/')[2]
        prefix = '/'.join(config['unprocessed_file_path'].split('/')[3:]) + '/'
        logger.info('path for incremental bookmarks : ' + prefix)
        print('path for incremental bookmarks : ' + prefix)
        
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_name)
        file_list = []
        
        for obj in bucket.objects.filter(Prefix=prefix):
            if '.csv' in obj.key:
                file_list.append("s3://" + bucket_name + '/' + obj.key)
        
        if not file_list:
            logger.info("No new files to process")
            return file_list
        else:
            logger.info("Files read from unprocessed folder in this run : " + str(len(file_list)) + " files")
            if config['number_of_files_to_read'] == '':
                logger.info("All files in unprocessed folder will be processed in this run.")
                print("All files in unprocessed folder will be processed in this run.")
                return file_list
            else:
                logger.info(str(config['number_of_files_to_read']) + " files will be processed in this run.")
                print(str(config['number_of_files_to_read']) + " files will be processed in this run.")
                return file_list[0:int(config['number_of_files_to_read'])]
    except Exception as e:
        logger.error(f"Error reading unprocessed files: {str(e)}")
        raise Exception("Error reading unprocessed files.")


def get_unprocessed_file_list(spark, unprocessed_list):
    """
    Get list of raw files to process based on unprocessed folder bookmarks.
    
    :param spark: Spark session object
    :param unprocessed_list: List of bookmark files
    :return: List of raw file paths to process
    """
    logger.info("Inside get unprocessed file list")
    print("Inside get unprocessed file list")
    try:
        unprocessed_folder_df = spark.read.option("header", "true") \
            .option("inferSchema", "true") \
            .format("csv") \
            .load(unprocessed_list).withColumn("filePath", concat_ws('/', col("bucketName"), col("fileName")))
        
        file_list = ['s3://' + x for x in unprocessed_folder_df.agg(collect_list(col("filePath"))).collect()[0][0]]
        return file_list
    except Exception as e:
        logger.error(f"Error getting list of unprocessed files: {str(e)}")
        raise Exception("Error getting list of unprocessed file.")


def move_unprocessed_to_processed(config, file_list):
    """
    Move processed files from unprocessed folder to processed folder.
    
    :param config: Configuration dictionary
    :param file_list: List of files to move
    """
    logger.info("Inside unprocessed to processed movement")
    try:
        bucket_name = config['processed_file_path'].split('/')[2]
        new_prefix = '/'.join(config['processed_file_path'].split('/')[3:])
        logger.info('new_prefix : ' + new_prefix)
        print('new_prefix : ' + new_prefix)
        
        old_prefix = '/'.join(config['unprocessed_file_path'].split('/')[3:])
        logger.info('old_prefix : ' + old_prefix)
        print('old_prefix : ' + old_prefix)
        
        s3_resource = boto3.resource('s3')
        
        for file in file_list:
            file_name = file.split('/')[-1]
            old_path = bucket_name + '/' + old_prefix + '/' + file_name
            new_path = new_prefix + '/' + file_name
            s3_resource.Object(bucket_name, new_path).copy_from(CopySource=old_path)
            s3_resource.Object(bucket_name, old_prefix + '/' + file_name).delete()
        
        logger.info(f"Moved {len(file_list)} files from unprocessed to processed folder")
    except Exception as e:
        logger.error(f"Error moving files: {str(e)}")
        raise Exception("Error moving files from unprocessed folder to processed folder.")


def audit_table_entry(spark, config, status, record_count_dict, epoch_ts, start_ts):
    """
    Add entry to audit table for job tracking.
    
    :param spark: Spark session object
    :param config: Configuration dictionary
    :param status: Job status ('Started', 'Completed', 'Failed')
    :param record_count_dict: Dictionary with record counts
    :param epoch_ts: Epoch timestamp of job start
    :param start_ts: Timestamp of job start
    """
    try:
        audit_table_columns = ['epoch_timestamp', 'source_system', 'table_name', 'status', 'insert_count',
                               'update_count', 'delete_count', 'record_start_sequence', 'record_end_sequence', 'log_file_name']

        data = [(epoch_ts, config['source_system'], config['table_name'], status, record_count_dict['I'],
                 record_count_dict['U'], record_count_dict['D'], record_count_dict['SQ'], record_count_dict['EQ'], log_file_name)]

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

        audit_table_path = config['audit_table_path'] + '/' + str(config['source_system']).lower() + '/' \
                           + str(config['table_name']).lower()

        audit_table_df.coalesce(1).write \
            .format('hudi') \
            .options(**audit_hudi_properties) \
            .mode('append') \
            .save(audit_table_path)
        
        logger.info('Audit table updated successfully.')
    except Exception as e:
        logger.error(f"Error updating audit table: {str(e)}")
        raise Exception("Error updating audit table.")


def create_spark_session(config):
    """
    Create Spark session with configuration from config file.
    
    :param config: Configuration dictionary
    :return: Spark session object
    """
    logger.info("Inside create spark session")
    try:
        conf = SparkConf()
        conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
        conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
        conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
        conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        
        spark_conf = dict(config['spark_properties'])
        for key in spark_conf.keys():
            conf.set(key, spark_conf[key])

        if 'application_name' in list(config.keys()):
            if config['application_name'] != '':
                app_name = config['application_name']
            else:
                app_name = 'DefaultApp'
        else:
            app_name = 'DefaultApp'

        spark = SparkSession \
            .builder \
            .config(conf=conf) \
            .appName(app_name) \
            .enableHiveSupport() \
            .getOrCreate()

        logger.info("Spark session object created")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {str(e)}")
        raise Exception("Error in Spark Session Creation.")


def read_file(spark, config, file, file_list_to_process):
    """
    Read and process DynamoDB JSON files with proper deserialization and flattening.
    Includes validation checkpoints to prevent data loss.
    
    :param spark: Spark session object
    :param config: Configuration dictionary
    :param file: Single file path (for full load)
    :param file_list_to_process: List of files to process (for incremental load)
    :return: Processed Pandas DataFrame
    """
    logger.info("Inside read file")
    print("Inside read file")
    try:
        if config['load_type'].lower() == 'full':
            # Full load: Read parquet files
            input_file_path = file
            logger.info("Reading input file from path : " + input_file_path)
            print("Reading input file from path : " + input_file_path)
            
            df = wr.s3.read_parquet(input_file_path)
            df1 = df.reset_index().drop('index', axis=1)
            
            # Validate initial data
            validate_dataframe(df1, "Initial Parquet Read")
            
            # Convert to list of dictionaries for normalization
            lst_dict = []
            for i in range(len(df1)):
                tpp = df1.loc[i].to_json()
                jsn = json.loads(tpp)
                lst_dict.append(jsn)
            
            # Initial normalization
            Tdf = pd.json_normalize(lst_dict)
            
            if 'sequence' in Tdf.columns:
                Tdf.drop(['sequence'], axis=1, inplace=True)
            
            validate_dataframe(Tdf, "After Initial Normalize")
            
            # Process nested structures
            tmp = Tdf.to_dict('records')
            
            # Normalize products with safe handling
            prdct = safe_normalize_nested(tmp, ['products'], 'products.', ['leadId'])
            validate_dataframe(prdct, "Products Normalized")
            
            # Normalize partyList with safe handling
            party = safe_normalize_nested(tmp, ['personalDetails', 'partyList'], 'personalDetails.partyList.', ['leadId'])
            validate_dataframe(party, "PartyList Normalized")
            
            # Merge with proper validation - use left join to preserve main data
            if 'personalDetails.partyList' in Tdf.columns:
                prefinal = safe_merge(Tdf, party, 'leadId', 'left')
                prefinal = prefinal.drop('personalDetails.partyList', axis=1, errors='ignore')
            else:
                prefinal = Tdf
            
            validate_dataframe(prefinal, "After PartyList Merge")
            
            if 'products' in prefinal.columns:
                fdf = safe_merge(prefinal, prdct, 'leadId', 'left')
                fdf = fdf.drop('products', axis=1, errors='ignore')
            else:
                fdf = prefinal
            
            validate_dataframe(fdf, "After Products Merge")
            
            # Normalize column names
            fdf = normalize_column_names(fdf)
            
            # Handle null values - preserve empty strings for missing data
            fnl = fdf.fillna('')
            
            validate_dataframe(fnl, "Final DataFrame")
            
        else:
            # Incremental load: Read JSON event files
            lst_dict = []
            seq_num = []
            input_file_path = file_list_to_process
            
            logger.info(f"Reading {len(input_file_path)} input files for incremental load")
            print("Reading input file from paths")
            
            files_processed = 0
            files_with_errors = 0
            
            for file_path in input_file_path:
                try:
                    df = wr.s3.read_json(file_path, use_threads=True)
                    lyst = df['Records'].tolist()
                    
                    for i in range(len(lyst)):
                        try:
                            # Get NewImage from DynamoDB stream record
                            if 'dynamodb' in lyst[i] and 'NewImage' in lyst[i]['dynamodb']:
                                newimage = lyst[i]['dynamodb']['NewImage']
                                jsn = from_dynamodb_to_json(newimage)
                                lst_dict.append(jsn)
                                
                                # Get sequence number
                                seq_no = lyst[i]['dynamodb'].get('SequenceNumber', '')
                                seq_num.append(seq_no)
                        except Exception as record_error:
                            logger.warning(f"Error processing record {i} in file {file_path}: {str(record_error)}")
                    
                    files_processed += 1
                except Exception as file_error:
                    logger.warning(f"Error reading file {file_path}: {str(file_error)}")
                    files_with_errors += 1
            
            logger.info(f"Files processed: {files_processed}, Files with errors: {files_with_errors}, Total records: {len(lst_dict)}")
            print(f"Files processed: {files_processed}, Total records: {len(lst_dict)}")
            
            if len(lst_dict) == 0:
                logger.warning("No records found in input files")
                return pd.DataFrame()
            
            # Create initial DataFrame
            Tdf = pd.json_normalize(lst_dict)
            
            if 'sequence' in Tdf.columns:
                Tdf.drop(['sequence'], axis=1, inplace=True)
            
            # Add sequence numbers
            Tdf['Sequence_No'] = seq_num
            
            validate_dataframe(Tdf, "After Initial Normalize (Incremental)")
            logger.info("Tdf is created")
            print("Tdf is created")
            
            # Process nested structures
            tmp = Tdf.to_dict('records')
            
            # Normalize products with safe handling
            prdct = safe_normalize_nested(tmp, ['products'], 'products.', ['leadId'])
            validate_dataframe(prdct, "Products Normalized (Incremental)")
            
            # Normalize partyList with safe handling  
            party = safe_normalize_nested(tmp, ['personalDetails', 'partyList'], 'personalDetails.partyList.', ['leadId'])
            validate_dataframe(party, "PartyList Normalized (Incremental)")
            
            # Merge with proper validation - use left join to preserve main data
            if 'personalDetails.partyList' in Tdf.columns:
                prefinal = safe_merge(Tdf, party, 'leadId', 'left')
                prefinal = prefinal.drop('personalDetails.partyList', axis=1, errors='ignore')
            else:
                prefinal = Tdf
            
            validate_dataframe(prefinal, "After PartyList Merge (Incremental)")
            
            if 'products' in prefinal.columns:
                fnl = safe_merge(prefinal, prdct, 'leadId', 'left')
                fnl = fnl.drop('products', axis=1, errors='ignore')
            else:
                fnl = prefinal
            
            validate_dataframe(fnl, "After Products Merge (Incremental)")
            
            # Handle null values - preserve empty strings
            fnl = fnl.fillna('')
            
            # Normalize column names
            fnl = normalize_column_names(fnl)
            
            validate_dataframe(fnl, "Final DataFrame (Incremental)")
            logger.info("Columns renaming is done")
            print("Columns renaming is done")
        
        return fnl
        
    except Exception as e:
        logger.error(f"Error reading input file: {str(e)}")
        logger.error(traceback.format_exc())
        raise Exception("Error reading input file.")


def column_chck(config, spark, initial_df):
    """
    Check and align columns with existing schema in destination.
    Handles schema evolution by adding missing columns.
    
    :param config: Configuration dictionary
    :param spark: Spark session object
    :param initial_df: Input Pandas DataFrame
    :return: DataFrame with aligned columns
    """
    output_file_path = config['destination_path'] + '/' + config['table_name'] + '/' + '*.parquet'
    logger.info('path to read for checking missing columns in raw data : ' + output_file_path)
    print('path to read for checking missing columns in raw data : ' + output_file_path)
    
    try:
        chk_df = wr.s3.read_parquet(output_file_path, chunked=True)
        chck = []
        
        for df in chk_df:
            col_list = df.columns.to_list()[5:]
            for cl in col_list:
                if cl not in chck:
                    chck.append(cl)
        
        cols = initial_df.columns.to_list()
        miss = list(set(chck) - set(cols))
        
        logger.info(f"Adding {len(miss)} missing columns to align with destination schema")
        
        for cl in miss:
            initial_df[cl] = ''
        
        # Also check for new columns that don't exist in destination
        new_cols = list(set(cols) - set(chck))
        if new_cols:
            logger.info(f"New columns in this batch: {len(new_cols)}")
        
        return initial_df
        
    except Exception as e:
        logger.warning(f"Could not read destination for column check (may be first run): {str(e)}")
        return initial_df


def full_fileread(config):
    """
    Get list of parquet files for full load.
    
    :param config: Configuration dictionary
    :return: List of parquet file paths
    """
    s3 = boto3.resource('s3')
    logger.info('getting bucket name from source path')
    print('getting bucket name from source path')
    
    bucket_name = config['source_path'].split('/')[2]
    bucket = s3.Bucket(bucket_name)
    logger.info(f"Bucket: {bucket_name}")
    print(f"Bucket: {bucket_name}")
    
    # Use prefix from config source_path, or fallback to default for compatibility
    if len(config['source_path'].split('/')) > 3:
        prefix = '/'.join(config['source_path'].split('/')[3:]) + '/'
    else:
        prefix = 'data/NeoProposal/Neorewiring-prod-lead/full20231101/'
    logger.info(f"Prefix: {prefix}")
    print(f"Prefix: {prefix}")
    
    file_list = []
    for obj in bucket.objects.filter(Prefix=prefix, Delimiter='/'):
        if '.parquet' in obj.key:
            file_list.append("s3://" + bucket_name + '/' + obj.key)
    
    logger.info(f"Found {len(file_list)} parquet files")
    print(f"Found {len(file_list)} parquet files")
    
    return file_list


def main():
    """
    Main entry point for the ETL job.
    Handles both full and incremental load types with proper validation.
    """
    logger.info("Inside main function")
    
    if len(sys.argv) != 2:
        logger.info(len(sys.argv))
        logger.info("Command line arguments : " + str(sys.argv))
        logger.info("Incorrect command line arguments.")
        exit(1)
    
    config = {}
    file_list_to_process = []
    file = None
    unprocessed_list = []
    job_status = ''
    record_count_dict_initial = {'I': 0, 'U': 0, 'D': 0, 'SQ': '0', 'EQ': '0'}
    record_count_dict_updated = {}
    epoch_ts = int(time.time())
    start_ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    
    try:
        # Read config file
        logger.info("Calling function to read config file")
        config = read_config(sys.argv[1])
        
        # Create Spark session
        logger.info("Calling function to create Spark session object")
        spark = create_spark_session(config)
        
        # Get list of files for incremental load
        if config['load_type'].lower() == 'incremental':
            logger.info("Calling function to get list of files in unprocessed folder.")
            print("Calling function to get list of files in unprocessed folder.")
            unprocessed_list = get_unprocessed_folder_file_list(config)
            
            if len(unprocessed_list) > 0:
                file_list_to_process = get_unprocessed_file_list(spark, unprocessed_list)
        
        # Process files
        if (config['load_type'].lower() == 'incremental' and len(file_list_to_process) > 0) or \
           (config['load_type'].lower() == 'full'):
            
            logger.info("Calling function to read input file")
            print("Calling function to read input file")
            
            if config['load_type'].lower() == 'full':
                # Full load processing
                files_read = full_fileread(config)
                
                for file in files_read:
                    initial_df = read_file(spark, config, file, file_list_to_process)
                    
                    if len(initial_df) == 0:
                        logger.warning(f"No data read from file: {file}")
                        continue
                    
                    # Convert to Spark DataFrame
                    initial_df = spark.createDataFrame(initial_df.astype(str))
                    initial_df = initial_df.withColumn('Sequence_No', F.lit(datetime.datetime.now().strftime("%Y%m%d%H%M%S")))
                    
                    # Write output
                    logger.info("Writing output dataframe")
                    print("Writing output dataframe")
                    write_file(initial_df, config, '')
            
            if config['load_type'].lower() == 'incremental' and len(file_list_to_process) > 0:
                # Incremental load with batch processing (10,000 files per batch)
                batch_size = 10000
                total_files = len(file_list_to_process)
                total_batches = (total_files + batch_size - 1) // batch_size
                
                logger.info(f"Processing {total_files} files in {total_batches} batches of {batch_size}")
                
                for batch_num, i in enumerate(range(0, total_files, batch_size)):
                    file_list = file_list_to_process[i:i + batch_size]
                    
                    logger.info(f"Processing batch {batch_num + 1}/{total_batches} with {len(file_list)} files")
                    print(f"Processing batch {batch_num + 1}/{total_batches}")
                    
                    initial_df = read_file(spark, config, None, file_list)
                    
                    if len(initial_df) == 0:
                        logger.warning(f"No data in batch {batch_num + 1}, skipping")
                        continue
                    
                    # Check and align columns with existing schema
                    initial_df = column_chck(config, spark, initial_df)
                    
                    # Convert to Spark DataFrame
                    initial_df = spark.createDataFrame(initial_df.astype(str))
                    
                    # Validate row count before write
                    row_count = initial_df.count()
                    logger.info(f"Batch {batch_num + 1}: Writing {row_count} rows")
                    
                    # Write output
                    logger.info("Writing output dataframe")
                    print("Writing output dataframe")
                    write_file(initial_df, config, '')
                    
                    # Move processed files
                    move_unprocessed_to_processed(config, unprocessed_list[i:i + batch_size])
                    
                    logger.info(f"Batch {batch_num + 1} completed successfully")
        
        else:
            logger.info("No files to process.")
        
        logger.info("Job Completed successfully")
        print("Job Completed successfully")
        logger.info("Calling function to update job end entry with successful status")
        print("Calling function to update job end entry with successful status")
        job_status = 'Success'

    except Exception as e:
        job_status = 'Failed'
        logger.error(e)
        logger.error(traceback.format_exc().replace('\n', '|'))
        logger.error("Calling function to update job end entry with failed status")
        print("Calling function to update job end entry with failed status")

    finally:
        # Upload log file
        upload_log_file(config)
        
        # Exit with appropriate code
        if job_status == 'Failed':
            exit(1)
        else:
            exit(0)


if __name__ == "__main__":
    logger.info("Calling main function")
    main()
