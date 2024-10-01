from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
import boto3
import concurrent.futures
from tqdm import tqdm
from botocore.config import Config
import gzip
import boto3
import io
import gc
import os
from multiprocessing import cpu_count


config = Config(
    retries = {
        'max_attempts': 10,
        'mode': 'standard'
    },
    max_pool_connections=120  # Increase the pool size
)
s3_client_1 = boto3.client(
    's3',
config = config
)


os.environ['PYSPARK_PYTHON'] = '/home/ubuntu/miniconda/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/home/ubuntu/miniconda/bin/python3'


def list_s3_files(bucket_name, prefix):
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    
    file_keys = []
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('.parquet'):
                    file_keys.append(obj['Key'])
    return file_keys



def download_contents(blob_id, src_encoding):
    # s3_url = f"s3://softwareheritage/content/{blob_id}"
    # s3_url = "s3://llm-spark/e380d116d6416bf303fbcacad06dec3a409d740f"

    bucket_name = 'softwareheritage'  # Replace with your actual bucket name

    try:
        # Fetch the object from S3
        response = s3_client_1.get_object(Bucket=bucket_name, Key=f'content/{blob_id}')
        compressed_data = response['Body'].read()
        
        # Decompress gzip-compressed data
        with gzip.GzipFile(fileobj=io.BytesIO(compressed_data)) as gzip_file:
            content = gzip_file.read().decode(src_encoding)
        
        # Return the content and word count
        return {"text": content, "word_count": len(content.split())}
    
    except Exception as e:
        # Handle exceptions (e.g., S3 errors, gzip errors, decoding errors)
        print(f"Error downloading or processing blob_id {blob_id}: {e}")
        return {"text": "", "word_count": 0}



def process_row_in_thread(blob_id, src_encoding):
    # This function is designed to be run in a thread
    content_data = download_contents(blob_id, src_encoding)
    return blob_id, content_data['text'], content_data['word_count']

def process_data_in_process(data_chunk):
    results = []
    max_threads =  25 # Number of threads per process

    # Thread pool within each process
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as thread_executor:
        future_to_row = {thread_executor.submit(process_row_in_thread, d['blob_id'], d['src_encoding']): d for d in data_chunk}
        
        for future in concurrent.futures.as_completed(future_to_row):
            try:
                row_result = future.result()
                results.append(row_result)
            except Exception as e:
                print(f"Error processing row: {e}")
    gc.collect()

    return results

def process_data_in_parallel(data, failed_try=0):
    # Split data into chunks based on the number of available CPU cores
    max_processes = int(min(cpu_count(), len(data))-20) # Limit the number of processes
    chunk_size = (len(data) // max_processes)  # Data chunk size for each process

    data_chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
    results = []

    print("MAx_worker", max_processes)
    # Process pool across all data chunks
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_processes) as process_executor:
        future_to_chunk = {process_executor.submit(process_data_in_process, chunk): chunk for chunk in data_chunks}
        
        for future in tqdm(concurrent.futures.as_completed(future_to_chunk), total=len(future_to_chunk), desc="Processing chunks in process pool"):

            try:
                chunk_results = future.result()
                results.extend(chunk_results)
            except Exception as e:
                failed_try+=1
                if failed_try<3:
                    print("Trying again")
                    results = process_data_in_parallel(data, failed_try=failed_try)
                print(f"Error processing chunk: {e}")
    gc.collect()

    return results

# Read the Parquet files from S3

def process_files(file, spark):
    file_name = file.split('/')[-1]
    print(f"Processing file: {file_name}")

    df = spark.read.option("mergeSchema", "true").parquet(f's3a://llm-spark/{file}')
    # df = spark.read.option("mergeSchema", "true").parquet(f'101.parquet')
    # df = df.repartition(120)  # Adjust number of partitions if needed

    # Initialize an empty DataFrame to accumulate results
    extr_schema = StructType([
        StructField("blob_id", StringType(), True),
        StructField("text", StringType(), True),
        StructField("word_count", IntegerType(), True)
    ])
    # results_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=extr_schema)

    # Process data in chunks
   
    total_rows = df.count()

    # Define the chunk size
    chunk_size = 800000  # Adjust this value as needed

    # Repartition the DataFrame to have smaller chunks
    df = df.repartition(int(total_rows / chunk_size) + 1, 'blob_id')
    s3_bucket = 'llm-spark'
    s3_path = 'stack_v2/processed_data'


    local_dir = f"./extr1/{file_name.split('.')[0]}"
    # for start in tqdm(range(0, total_rows, chunk_size)):
    for partition_id in range(df.rdd.getNumPartitions()):
        # partition_id = 13
        try:
            print(f"Processing partition {partition_id + 1} of {df.rdd.getNumPartitions()}")

            # Extract the specific partition
            partition_df = df.rdd.mapPartitionsWithIndex(
                lambda idx, it: it if idx == partition_id else [],
                preservesPartitioning=True
            ).toDF()

            data = partition_df.select('blob_id', 'src_encoding').collect()
            # end = min(start + chunk_size, total_rows)
            # chunk_df = df.limit(end).offset(start)  # Fetch the chunk of data
            # data = chunk_df.select('blob_id', 'src_encoding').collect()

            # Process the chunk and create a DataFrame
            results = process_data_in_parallel(data)

            
            chunk_results_df = spark.createDataFrame(results, schema=extr_schema)
            print("Result Count", chunk_results_df.count())
            
            # Append results to the main DataFrame
            # results_df = results_df.union(chunk_results_df)
            del results
            # del chunk_results_df
            local_file = f"{local_dir}/{partition_id}.parquet"
            print("File is writing")
            partition_df.join(chunk_results_df, on=['blob_id'], how='left')\
                .write.mode('overwrite')\
                    .parquet(local_file, compression="snappy")

            del partition_df
            del chunk_results_df
        except Exception as e:
            print("Error Occured", str(e))


    print(f"Uploading directory {local_dir} to s3://{s3_bucket}/{s3_path}/{file_name.split('.')[0]}")
    s3_client = boto3.client('s3')
    for root, dirs, files in os.walk(local_dir):
        files_to_upload = [os.path.join(root, file) for file in files]
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as upload_executor:
            futures = [upload_executor.submit(s3_client.upload_file, file, s3_bucket, f"{s3_path}/{file_name.split('.')[0]}/{os.path.relpath(file, local_dir)}") for file in files_to_upload]
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Uploading files"):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error uploading file: {e}")

    # df = df.join(results_df, on=['blob_id'], how='inner')

    # # Show the resulting DataFrame
    # # df.count()
    
    # df.write.mode('overwrite').parquet(f'./extr1/{file_name}', compression="snappy")


def main():
    files = list_s3_files('llm-spark', 'Stack_V2')
    files = [file for file in files if 'Stack_V2-extr' not in file]
     
    files.sort()

    
    spark = SparkSession.builder \
    .appName("Read S3 Parquet").config("spark.sql.legacy.parquet.nanosAsLong", "true") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "300000") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "200") \
    .config("spark.hadoop.fs.s3a.access.key", "") \
    .config("spark.hadoop.fs.s3a.secret.key", "") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.driver.memory", "100g") \
    .config("spark.executor.memory", "10g") \
    .config("spark.executor.cores", "1") \
    .config("spark.num.executors", "120") \
    .config("spark.sql.shuffle.partitions", "1000") \
    .config("spark.memory.fraction", "0.8") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/aws-java-sdk-bundle-1.12.375.jar:/opt/spark/jars/hadoop-aws-3.3.1.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/aws-java-sdk-bundle-1.12.375.jar:/opt/spark/jars/hadoop-aws-3.3.1.jar") \
    .getOrCreate()
    for file_x in files[270:360]:
        print("file_name", file_x)
        process_files(file_x, spark)

if __name__ =='__main__':
    main()


    # 6:47 -5
    # 7:16
    # 7:33o
