import traceback
import json
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.functions import col, concat_ws, when, collect_list, lit, to_timestamp
from pyspark.sql.functions import year, month, date_format, explode_outer
from pyspark.sql.types import StringType, ArrayType, StructType
import sys
import logging
import datetime
import time

sys.path.append('/home/hadoop')
import boto3
from boto3.dynamodb.types import TypeDeserializer
# creating log file name
log_file_name = 'job_' + str(datetime.datetime.now().strftime('%Y%m%d_%H%M%S_%f')) + '.log'
extra = {'log_file_name': log_file_name}
logger = logging.getLogger(__name__)
syslog = logging.FileHandler(log_file_name, mode='w')
formatter = logging.Formatter('%(log_file_name)s;%(asctime)s;%(levelname)s;%(message)s')
syslog.setFormatter(formatter)
logger.setLevel(logging.INFO)
logger.addHandler(syslog)
logger = logging.LoggerAdapter(logger, extra)
# For Converting DynamoDB json to Simple Json
def from_dynamodb_to_json(item):
    d = TypeDeserializer()
    res = {k: d.deserialize(value=v) for k, v in item.items()}
    return res


# Complex nested json parsing
def pds(v):
    g = []

    def print_dict(v, prefix=''):

        if isinstance(v, dict):
            # For parsing dict type structure
            for k, v2 in v.items():
                p2 = "{}.{}".format(prefix, k)
                print_dict(v2, p2)
        elif isinstance(v, list):
            # For parsing list type structure
            for i, v2 in enumerate(v):
                p2 = "{}.{}".format(prefix, i)
                print_dict(v2, p2)
        else:
            # Oth Level parsing
            g.append(["{}".format(prefix), v])

    print_dict(v)
    return g


def count(s, c):
    # Count variable
    res = 0

    for i in range(len(s)):

        # Checking character in string
        if s[i] == c:
            res = res + 1
    return res

def parsing(df):
    # Column names passed to a list
    df_col = df.columns.values.tolist()
    lyst = []
    for col in df_col:
        st = col.split(".")
        ncol = '_'.join(map(str,st[1:]))
        lyst.append(ncol)
    
    df.columns=lyst
    return df

def read_config(config_path):
    """
    This function takes path as input and return config dictionary.
    :param config_path: path for json file with config details
    :return: config details dictionary
    """
    logger.info("Inside read config")
    try:
        # checking if config path provided as input is s3 path or file system path
        if config_path[0:2] == 's3':
            # read config file from s3
            logger.info("Reading config file from S3")
            s3 = boto3.resource('s3')
            file_object = s3.Object(config_path.split('/')[2], '/'.join(config_path.split('/')[3:]))
            file_content = file_object.get()['Body'].read().decode('utf-8')
            # converting file content to json format
            json_content = json.loads(file_content)
            json_object = json.dumps(json_content)
        else:
            # reading config file from system
            logger.info("Reading config file from path : " + config_path)
            # converting file content to json format
            json_content = json.load(open(config_path, 'r'))
            json_object = json.dumps(json_content)
            logger.info("Input Config Details:")
        logger.info(json_object)
        return json_content
    except Exception as e:
        raise Exception("Error reading config.")


def get_record_count(spark, initial_df, config):
    """
    This function takes spark session object and initial dataframe as input and returns a dictionary with insert,
    update and delete count.
    :param config: Input config
    :param spark: spark session object
    :param initial_df: initial dataframe
    :return: dictionary of insert, update and delete record count
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
                if row_dict['SQ'] in ['', ' ']:
                    record_type_counts[key] = '0'
                else:
                    record_type_counts[key] = str(row_dict['SQ'])
            elif key == 'EQ':
                if row_dict['EQ'] in ['', ' ']:
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
        raise Exception("Error getting record count.")


def write_file(df, config, write_type):
    """
    This function takes dataframe and json config as input and writes the dataframe to destination path.
    :param write_type: type of dataset to write
    :param df: spark dataframe to write on storage
    :param config: config dictionary provided as json file to this job
    :return: NA
    """
    logger.info("Inside write file")
    print("Inside write file")
    try:
        # write file to s3 if it is error dataframe
        if write_type == 'error':
            error_file_path = config['error_records_destination_path']
            logger.info("Writing error records to path : " + error_file_path)
            df.coalesce(1).write \
                .partitionBy("source_system", "table_name", "year", "month", "day") \
                .format('parquet') \
                .mode('append') \
                .save(error_file_path)
        # write processed file to s3
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
        raise Exception("Error writing output file.")

def upload_log_file(config):
    """
    This method takes input config and uploads log file to S3 bucket.
    :param config: config dictionary provided as json file to this job
    :return: NA
    """
    logger.info("Inside upload file")
    # create s3 client
    s3_client = boto3.client('s3')
    # get bucket name from log file path provided in config file
    bucket_name = config['log_file_path'].split('/')[2]
    file_name = '/'.join(config['log_file_path'].split('/')[3:])
    # creating complete log file path for s3 upload
    filepath = file_name + '/' + config['source_system'].lower() + '/' + config['table_name'] + '/' + log_file_name
    logger.info("Log File Uploaded")
    # uploading log file to s3
    response = s3_client.upload_file(log_file_name, bucket_name, filepath)
    print("Log File Upload Response : ", response)

def get_unprocessed_folder_file_list(config):
    """
    This function takes input config dictionary and returns list of files in unprocessed folder
    :param config: config dictionary provided as json file to this job
    :return: list of unprocessed files
    """
    logger.info("Inside unprocessed folder file list")
    print("Inside unprocessed folder file list")
    try:
        # getting bucket name from unprocessed file path
        bucket_name = config['unprocessed_file_path'].split('/')[2]
        # getting prefix i.e. folder path for unprocessed files
        prefix = '/'.join(config['unprocessed_file_path'].split('/')[3:]) + '/'
        logger.info('path for incremental bookmarks : ' + prefix)
        print('path for incremental bookmarks : ' + prefix)
        #logger.info("Prefix is: "+ str(prefix))
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_name)
        file_list = []
        # getting list of files from unprocessed s3 folder
        for obj in bucket.objects.filter(Prefix=prefix):
            # we only need files which are in csv format
            if '.csv' in obj.key:
                file_list.append("s3://" + bucket_name + '/' + obj.key)
        # if there is no file to process then raise exception
        if not file_list:
            logger.info("No new files to process")
            return file_list
        else:
            logger.info("Files read from unprocessed folder in this run : " + str(file_list))
            if config['number_of_files_to_read'] == '':
                logger.info("All files in unprocessed folder will be processed in this run.")
                print("All files in unprocessed folder will be processed in this run.")
                return file_list
            else:
                logger.info(str(config['number_of_files_to_read']) + " files will be processed in this run.")
                print(str(config['number_of_files_to_read']) + " files will be processed in this run.")
                return file_list[0:int(config['number_of_files_to_read'])]
    except Exception as e:
        raise Exception("Error reading unprocessed files.")

def get_unprocessed_file_list(spark, unprocessed_list):
    """
    This function takes list of files in unprocessed folder as input and returns list of files to read from raw folder.
    :param spark: spark session object
    :param unprocessed_list: List of files in unprocessed folder
    :return: List of files to process from raw bucket
    """
    logger.info("Inside get unprocessed file list")
    print("Inside get unprocessed file list")
    try:
        # geting list of files that we have to process from raw to curated in this run
        #print(unprocessed_list)
        unprocessed_folder_df = spark.read.option("header", "true") \
            .option("inferSchema", "true") \
            .format("csv") \
            .load(unprocessed_list).withColumn("filePath", concat_ws('/', col("bucketName"), col("fileName")))
        #unprocessed_folder_df.printSchema()
        file_list = ['s3://' + x for x in unprocessed_folder_df.agg(collect_list(col("filePath"))).collect()[0][0]]
        #logger.info(file_list)
        #print(file_list)
        #logger.info("Files to be processed in this run : " + str(file_list))
        return file_list
    except Exception as e:
        raise Exception("Error getting list of unprocessed file.")
'''
def move_unprocessed_to_processed(config, file_list):
    """
    This method moves files from unprocessed folder to processed folder
    :param file_list: List of files to move from unprocessed to processed
    :param config: config dictionary provided as json file to this job
    :return: NA
    """
    logger.info("Inside unprocessed to processed movement")
    try:
        # get bucket name from input processed file path
        bucket_name = config['processed_file_path'].split('/')[2]
        # creating new prefix i.e. folder where we have to move our files from unprocessed folder
        new_prefix = '/'.join(config['processed_file_path'].split('/')[3:]) 
        # creating unprocessed file path
        old_prefix = '/'.join(config['unprocessed_file_path'].split('/')[3:])
        # creating resource object
        s3_resource = boto3.resource('s3')
        logger.info("File to be moved from unprocessed to processed folder : " + str(file_list))

        client = boto3.client('s3')
        delete_us = dict(Objects=[])
        for file in file_list:
            key = '/'.join(file.split('/')[3:])
            delete_us['Objects'].append(dict(Key=key))
            if len(delete_us['Objects']) >= 1000:
                logger.info("1000 getting removed")
                client.delete_objects(Bucket=bucket_name, Delete=delete_us)
                delete_us = dict(Objects=[])
        # moving files from unprocessed to processed folder and deleting from unprocessed folder
        #for file in file_list:
        #    file_name = file.split('/')[-1]
        #    old_path = bucket_name + '/' + old_prefix + '/' + file_name
        #    new_path = new_prefix + '/' + file_name
        #    s3_resource.Object(bucket_name, new_path).copy_from(
        #        CopySource=old_path)
        #    s3_resource.Object(bucket_name, old_prefix + '/' + file_name).delete()
    except Exception as e:
        raise Exception("Error moving files from unprocessed folder to processed folder.")
'''

def move_unprocessed_to_processed(config, file_list):
    """
    This method moves files from unprocessed folder to processed folder
    :param file_list: List of files to move from unprocessed to processed
    :param config: config dictionary provided as json file to this job
    :return: NA
    """
    logger.info("Inside unprocessed to processed movement")
    try:
        # get bucket name from input processed file path
        bucket_name = config['processed_file_path'].split('/')[2]
        # creating new prefix i.e. folder where we have to move our files from unprocessed folder
        new_prefix = '/'.join(config['processed_file_path'].split('/')[3:])
        logger.info('new_prefix : '+new_prefix)
        print('new_prefix : '+new_prefix)
        # creating unprocessed file path
        old_prefix = '/'.join(config['unprocessed_file_path'].split('/')[3:])
        logger.info('old_prefix : '+old_prefix)
        print('old_prefix : '+old_prefix)
        # creating resource object
        s3_resource = boto3.resource('s3')
        #logger.info("File to be moved from unprocessed to processed folder : " + str(file_list))
        #print("File to be moved from unprocessed to processed folder : " + str(file_list))
        # moving files from unprocessed to processed folder and deleting from unprocessed folder
        for file in file_list:
            file_name = file.split('/')[-1]
            old_path = bucket_name + '/' + old_prefix + '/' + file_name
            new_path = new_prefix + '/' + file_name
            s3_resource.Object(bucket_name, new_path).copy_from(
                CopySource=old_path)
            s3_resource.Object(bucket_name, old_prefix + '/' + file_name).delete()
    except Exception as e:
        raise Exception("Error moving files from unprocessed folder to processed folder.")

def audit_table_entry(spark, config, status, record_count_dict, epoch_ts, start_ts):
    """
    This function adds a new entry in audit table
    :param record_count_dict: This dictionary contains the insert, update, delete, start sequence, end sequence,
    start timestamp and end timestamp
    :param epoch_ts: epoch time of job start
    :param start_ts: timestamp of job start
    :param spark: spark session object
    :param config: config dictionary provided as json file to this job
    :param status: job status 'C', 'S' and 'F'
    :return: NA
    """
    try:
        # creating columns list for audit table
        audit_table_columns = ['epoch_timestamp', 'source_system', 'table_name', 'status', 'insert_count',
                               'update_count', 'delete_count', 'record_start_sequence', 'record_end_sequence','log_file_name']

        data = [(epoch_ts, config['source_system'], config['table_name'], status, record_count_dict['I'],
                 record_count_dict['U'], record_count_dict['D'], record_count_dict['SQ'], record_count_dict['EQ'], log_file_name)]

        audit_table_df = spark.createDataFrame(data).toDF(*audit_table_columns)

        # creating hudi properties for audit table
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

        # writing audit table
        audit_table_df.coalesce(1).write \
            .format('hudi') \
            .options(**audit_hudi_properties) \
            .mode('append') \
            .save(audit_table_path)
        logger.info('Audit table updated successfully.')

    except Exception as e:
        raise Exception("Error updating audit table.")


def create_spark_session(config):
    """
    This function takes json config as input and return a spark session object.
    :param config: config dictionary provided as json file to this job
    :return: spark session object
    """
    logger.info("Inside create spark session")
    try:
        conf = SparkConf()
        conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
        conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
        conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
        conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        # setting spark configuration properties provided in config file
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

        # creating spark session
        spark = SparkSession \
            .builder \
            .config(conf=conf) \
            .appName(app_name) \
            .enableHiveSupport() \
            .getOrCreate()

        logger.info("Spark session object created")
        return spark
    except Exception as e:
        raise Exception("Error in Spark Session Creation.")

def flatten_struct_columns(df, prefix=""):
    """
    Recursively flatten struct columns in a PySpark DataFrame.
    :param df: PySpark DataFrame
    :param prefix: prefix for nested column names
    :return: list of flattened column expressions
    """
    flat_cols = []
    for field in df.schema.fields:
        col_name = f"{prefix}{field.name}" if prefix else field.name
        if hasattr(field.dataType, 'fields'):
            # It's a struct, recurse through nested fields
            for nested_field in field.dataType.fields:
                nested_col_name = f"{col_name}_{nested_field.name}"
                if hasattr(nested_field.dataType, 'fields'):
                    # Further nested struct - handle recursively
                    flat_cols.append(col(f"`{field.name}`.`{nested_field.name}`").alias(nested_col_name))
                else:
                    flat_cols.append(col(f"`{field.name}`.`{nested_field.name}`").alias(nested_col_name))
        else:
            flat_cols.append(col(f"`{field.name}`").alias(col_name))
    return flat_cols


def process_nested_json_spark(spark, base_df):
    """
    Process nested JSON structures (products, personalDetails.partyList) using PySpark.
    :param spark: spark session object
    :param base_df: base PySpark DataFrame with nested structures
    :return: flattened PySpark DataFrame
    """
    result_df = base_df
    
    # Get all column names for later reference
    all_columns = result_df.columns
    
    # Handle 'products' array if it exists
    if 'products' in all_columns:
        # Check if products is an array type
        products_field = next((f for f in result_df.schema.fields if f.name == 'products'), None)
        if products_field and isinstance(products_field.dataType, ArrayType):
            # Explode products array
            products_exploded = result_df.select(
                col('leadId'),
                explode_outer(col('products')).alias('product_item')
            )
            
            # Flatten the product struct if it exists
            if products_exploded.schema['product_item'].dataType and hasattr(products_exploded.schema['product_item'].dataType, 'fields'):
                product_cols = [col('leadId')]
                for field in products_exploded.schema['product_item'].dataType.fields:
                    product_cols.append(col(f'product_item.{field.name}').alias(f'products_{field.name}'))
                products_flat = products_exploded.select(product_cols)
            else:
                products_flat = products_exploded.withColumnRenamed('product_item', 'products_item')
            
            # Drop products column from result and join with flattened products
            result_df = result_df.drop('products')
            result_df = result_df.join(products_flat, on='leadId', how='outer')
    
    # Handle 'personalDetails.partyList' - first check if personalDetails exists
    if 'personalDetails' in result_df.columns:
        pd_field = next((f for f in result_df.schema.fields if f.name == 'personalDetails'), None)
        if pd_field and hasattr(pd_field.dataType, 'fields'):
            # Check if partyList exists in personalDetails
            party_list_field = next((f for f in pd_field.dataType.fields if f.name == 'partyList'), None)
            if party_list_field and isinstance(party_list_field.dataType, ArrayType):
                # Explode partyList array
                party_exploded = result_df.select(
                    col('leadId'),
                    explode_outer(col('personalDetails.partyList')).alias('party_item')
                )
                
                # Flatten the party struct
                if party_exploded.schema['party_item'].dataType and hasattr(party_exploded.schema['party_item'].dataType, 'fields'):
                    party_cols = [col('leadId')]
                    for field in party_exploded.schema['party_item'].dataType.fields:
                        party_cols.append(col(f'party_item.{field.name}').alias(f'personalDetails_partyList_{field.name}'))
                    party_flat = party_exploded.select(party_cols)
                else:
                    party_flat = party_exploded.withColumnRenamed('party_item', 'personalDetails_partyList_item')
                
                # Flatten other personalDetails fields (excluding partyList)
                for field in pd_field.dataType.fields:
                    if field.name != 'partyList':
                        alias_name = f'personalDetails_{field.name}'
                        result_df = result_df.withColumn(alias_name, col(f'personalDetails.{field.name}'))
                
                # Drop personalDetails and join with flattened party
                result_df = result_df.drop('personalDetails')
                result_df = result_df.join(party_flat, on='leadId', how='outer')
    
    return result_df


def rename_columns_with_dots(df):
    """
    Rename columns replacing dots with underscores.
    :param df: PySpark DataFrame
    :return: PySpark DataFrame with renamed columns
    """
    new_columns = []
    for c in df.columns:
        new_columns.append(c.replace('.', '_'))
    return df.toDF(*new_columns)


def read_file(spark, config, file, file_list_to_process):
    """
    This function takes spark session object and json config as input and return initial dataframe created from file
    read from source path.
    :param file_list_to_process: List of files to process from raw to curated
    :param spark: spark session object
    :param config: config dictionary provided as json file to this job
    :return: spark dataframe
    """
    logger.info("Inside read file")
    print("Inside read file")
    try:
        # read files in case of full load
        if config['load_type'].lower() == 'full':
            # this part will read all the files provided in provided path with config provided extension
            input_file_path = file
            logger.info("Reading input file from path : " + input_file_path)
            print("Reading input file from path : " + input_file_path)
            # reading all the files with provided input format type in source path using PySpark
            df = spark.read.parquet(input_file_path)
            
            # Drop 'sequence' column if it exists
            if 'sequence' in df.columns:
                df = df.drop('sequence')
            
            # Process nested JSON structures using PySpark
            fnl = process_nested_json_spark(spark, df)
            
            # Rename columns replacing dots with underscores
            fnl = rename_columns_with_dots(fnl)
            
            # Fill null values with empty string
            fnl = fnl.na.fill('')
            
        # read file in case of incremental load
        else:
            # this part will read list of files which are there in unprocessed folder
            input_file_path = file_list_to_process
            logger.info("Reading input file from paths")
            print("Reading input file from paths")
            
            # Read all JSON files using PySpark
            json_df = spark.read.json(input_file_path)
            
            # Explode the Records array to process each record
            records_df = json_df.select(explode_outer(col('Records')).alias('record'))
            
            # Extract NewImage and SequenceNumber from each record
            records_with_seq = records_df.select(
                col('record.dynamodb.NewImage').alias('NewImage'),
                col('record.dynamodb.SequenceNumber').alias('Sequence_No')
            )
            
            # Flatten the NewImage struct - this contains the DynamoDB JSON format
            # We need to extract the actual values from the DynamoDB type wrappers (S, N, etc.)
            if 'NewImage' in records_with_seq.columns:
                new_image_field = next((f for f in records_with_seq.schema.fields if f.name == 'NewImage'), None)
                if new_image_field and hasattr(new_image_field.dataType, 'fields'):
                    select_cols = [col('Sequence_No')]
                    for field in new_image_field.dataType.fields:
                        # DynamoDB stores values as {"S": "value"} or {"N": "123"}, etc.
                        # Try to extract the actual value
                        if hasattr(field.dataType, 'fields'):
                            # Check for common DynamoDB types
                            for subfield in field.dataType.fields:
                                if subfield.name in ['S', 'N', 'B', 'BOOL', 'NULL', 'M', 'L', 'SS', 'NS', 'BS']:
                                    select_cols.append(col(f'NewImage.{field.name}.{subfield.name}').alias(field.name))
                                    break
                            else:
                                # If no DynamoDB type wrapper found, just use the struct
                                select_cols.append(col(f'NewImage.{field.name}').alias(field.name))
                        else:
                            select_cols.append(col(f'NewImage.{field.name}').alias(field.name))
                    
                    base_df = records_with_seq.select(select_cols)
                else:
                    base_df = records_with_seq
            else:
                base_df = records_with_seq
            
            logger.info("Out of the Loop")
            print("Out of the Loop")
            
            # Drop 'sequence' column if it exists
            if 'sequence' in base_df.columns:
                base_df = base_df.drop('sequence')
            
            logger.info("Tdf is created")
            print("Tdf is created")
            
            # Process nested JSON structures (products, personalDetails.partyList)
            fnl = process_nested_json_spark(spark, base_df)
            
            logger.info("final df is created")
            print("final df is created")
            
            # Fill null values with empty string
            fnl = fnl.na.fill('')
            
            # Rename columns replacing dots with underscores
            fnl = rename_columns_with_dots(fnl)
            
            logger.info("Columns renaming is done")
            print("Columns renaming is done")
        
        return fnl
    except Exception as e:
        raise Exception("Error reading input file.")

def column_chck(config, spark, initial_df):
    """
    Check for missing columns in the raw data compared to the destination schema.
    :param config: config dictionary
    :param spark: spark session object
    :param initial_df: PySpark DataFrame to check and update
    :return: PySpark DataFrame with missing columns added
    """
    output_file_path = config['destination_path'] + '/' + config['table_name'] + '/'
    logger.info('path to read for checking missing columns in raw data : ' + output_file_path)
    print('path to read for checking missing columns in raw data : ' + output_file_path)
    
    # Read parquet files using PySpark
    chk_df = spark.read.parquet(output_file_path)
    
    # Get all column names from the destination schema (skip first 5 columns)
    chck = chk_df.columns[5:]
    
    # Get columns from initial_df
    cols = initial_df.columns
    
    # Find missing columns
    miss = list(set(chck) - set(cols))
    
    # Add missing columns with empty string values using PySpark
    for cl in miss:
        initial_df = initial_df.withColumn(cl, lit(""))
    
    #logger.info('columns appended are:' + str(miss))
    #print('columns appended are:' + str(miss))
    return initial_df

def full_fileread(config):
    s3_client = boto3.client('s3')
    s3 = boto3.resource('s3')
    logger.info('getting bucket name from unprocessed file path')
    print('getting bucket name from unprocessed file path')
    bucket_name = config['source_path'].split('/')[2]
    bucket = s3.Bucket(bucket_name)
    logger.info(bucket)
    print(bucket)
    #prefix = '/'.join(config['source_path'].split('/')[3:]) + '/'
    prefix = 'data/NeoProposal/Neorewiring-prod-lead/full20231101/'
    logger.info(prefix)
    print(prefix)
    file_list = []
    for obj in bucket.objects.filter(Prefix=prefix , Delimiter = '/'):
        if '.parquet' in obj.key:
            file_list.append("s3://" + bucket_name + '/' + obj.key)
    logger.info(file_list)
    print(file_list)
    return file_list

def main():
    """
    This is main function.
    :return: NA
    """
    logger.info("Inside main function")
    if len(sys.argv) != 2:
        logger.info(len(sys.argv))
        logger.info("Command line arguments : " + str(sys.argv))
        logger.info("Incorrect command line arguments.")
        exit(1)
    
    config = {}
    file_list_to_process = []
    file= None
    unprocessed_list = []
    job_status = ''
    record_count_dict_initial = {'I': 0, 'U': 0, 'D': 0, 'SQ': '0', 'EQ': '0'}
    record_count_dict_updated = {}
    epoch_ts = int(time.time())
    start_ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    try:
        # reading json config file
        logger.info("Calling function to read config file")
        config = read_config(sys.argv[1])
        
        # creating spark session
        logger.info("Calling function to create Spark session object")
        spark = create_spark_session(config)
        
        # entry in audit table
        #logger.info("Calling function to do job start entry in audit table")
        #audit_table_entry(spark, config, 'Started', record_count_dict_initial, epoch_ts, start_ts)
        
        # get list of files to read
        if config['load_type'].lower() == 'incremental':
            logger.info("Calling function to get list of files in unprocessed folder.")
            print("Calling function to get list of files in unprocessed folder.")
            unprocessed_list = get_unprocessed_folder_file_list(config)
            if len(unprocessed_list) > 0:
                file_list_to_process = get_unprocessed_file_list(spark, unprocessed_list)
        
        if (config['load_type'].lower() == 'incremental' and len(file_list_to_process) > 0) or (
                config['load_type'].lower() == 'full'):
            # reading input file
            logger.info("Calling function to read input file")
            print("Calling function to read input file")
            if (config['load_type'].lower() == 'full'):
                files_read = full_fileread(config)
                for file in files_read:
                    initial_df = read_file(spark, config, file, file_list_to_process)
                    # Cast all columns to string type
                    initial_df = initial_df.select([col(c).cast(StringType()).alias(c) for c in initial_df.columns])
                    initial_df = initial_df.withColumn('Sequence_No', F.lit(datetime.datetime.now().strftime("%Y%m%d%H%M%S")))
                    # get insert, update and delete record count
                    # record_count_dict_updated = get_record_count(spark, initial_df, config)
                    # writing input file dataframe
                    logger.info("Writing output dataframe")
                    print("Writing output dataframe")
                    write_file(initial_df, config, '')
            
            if (config['load_type'].lower() == 'incremental' and len(file_list_to_process) > 0):
                for i in range(0, len(file_list_to_process), 10000):
                    file_list = file_list_to_process[i:i+10000]
                    initial_df = read_file(spark, config, file, file_list)
                    initial_df = column_chck(config, spark, initial_df)
                    # Cast all columns to string type
                    initial_df = initial_df.select([col(c).cast(StringType()).alias(c) for c in initial_df.columns])
                    # get insert, update and delete record count
                    # record_count_dict_updated = get_record_count(spark, initial_df, config)
                    # writing input file dataframe
                    logger.info("Writing output dataframe")
                    print("Writing output dataframe")
                    write_file(initial_df, config, '')
                    move_unprocessed_to_processed(config, unprocessed_list[i:i+10000])
        
        else:
            logger.info("No files to process.")
            #record_count_dict_updated = {'I': 0, 'U': 0, 'D': 0, 'SQ': '0', 'EQ': '0'}
        
        logger.info("Job Completed successfully")
        print("Job Completed successfully")
        # update in audit table for successful job completion
        logger.info("Calling function to update job end entry with successful status")
        print("Calling function to update job end entry with successful status")
        #logger.info("Updating audit table with details : " + str(record_count_dict_updated))
        #audit_table_entry(spark, config, 'Completed', record_count_dict_updated, epoch_ts, start_ts)
        job_status = 'Success'

    except Exception as e:
        job_status = 'Failed'
        logger.error(e)
        logger.error(traceback.format_exc().replace('\n', '|'))
        # update in audit table for failed job
        logger.error("Calling function to update job end entry with failed status")
        print("Calling function to update job end entry with failed status")
        #logger.error("Updating audit table with details : " + str(record_count_dict_initial))
        #audit_table_entry(spark, config, 'Failed', record_count_dict_initial, epoch_ts, start_ts)

    finally:
        # upload log file in any case
        upload_log_file(config)
        # exiting with code 1 in case of failure and 0 in case of success
        if job_status == 'Failed':
            exit(1)
        else:
            exit(0)

if __name__ == "__main__":
    # calling main function
    logger.info("Calling main function")
    main()
