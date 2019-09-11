"""
etl_job.py
~~~~~~~~~~

    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --py-files packages.zip \
    --packages mysql:mysql-connector-java:8.0.15 \
    --files configs/etl_config.json \
    jobs/etl_job.py

A packages.zip contains Python modules required by ETL job (in
this example it contains a class to provide access to Spark's logger),
which need to be made available to each executor process on every node
in the cluster; --packages to use external packages such as mysql-connector;
etl_config.json is a text file sent to the cluster,
containing a JSON object with all of the configuration parameters
required by the ETL job; and, etl_job.py contains the Spark application
to be executed by a driver process on the Spark master node.

For more details on submitting Spark applications, please see here:
http://spark.apache.org/docs/latest/submitting-applications.html

"""

from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.functions import regexp_replace, split
from pyspark.sql.functions import json_tuple, arrays_zip  # Spark >= 2.4
from pyspark.sql.functions import from_utc_timestamp, unix_timestamp

from dependencies.spark import start_spark


def main():
    """Main ETL script definition.

    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='my_etl_job',
        files=['configs/etl_config.json'])

    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # no KeyError check here; below values must be present
    db_insert = config['mysql']['insert']
    file_write = config['file']['write']
    load_path = config['file']['load']['path']
    load_db_info = config['mysql']['load']
    save_db_info = config['mysql']['save']
    save_path = config['file']['save']['path']
    types = config['transform']

    # execute ETL pipeline
    log_data = extract_data(spark, load_path)
    category_data = extract_data_from_db(spark, load_db_info)
    data_transformed = transform_data(log_data, category_data, types)

    if db_insert:
        insert_data(data_transformed, save_db_info)
    if file_write:
        write_data(data_transformed, save_path)

    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')
    spark.stop()
    return None


def extract_data(spark, load_path):
    """Load data from Parquet file format.

    :param spark: Spark session object.
    :param load_path: Log files path
    :return: Spark DataFrame.
    """
    df = (
        spark
        .read
        .parquet(load_path))
    return df


def extract_data_from_db(spark, load_db_info):
    """Load data from MySQL

    :param spark: Spark session object.
    :param load_db_info: MySQL Connection Info.
    :return: Spark DataFrame
    """
    df = (
        spark
        .read
        .format('jdbc')
        .options(**load_db_info)
        .load())
    return df


def insert_data(df, save_db_info):
    """Insert data to MySQL

    :param df: Spark DataFrame
    :param save_db_info: MySQL Connection Info.
    :return: None
    """
    (
        df.
        write
        .format('jdbc')
        .options(**save_db_info)
        .mode('append')
        .save())
    return None


def explode_outer(col):
    sc = SparkContext._active_spark_context
    _explode_outer = sc._jvm.org.apache.spark.sql.functions.explode_outer
    return Column(_explode_outer(_to_java_column(col)))


def transform(self, f):
    return f(self)


DataFrame.transform = transform


def transform_data(log_data, category_data, types):
    """Transform Input DataFrame accordingly

    :param log_data: Input log DataFrame
    :param category_data: Input category DataFrame
    :param types: preprocess types
    :return: Final Output Format DataFrame
    """
    return (
        log_data
        .transform(lambda df: select_default(df, types['default']))
        .transform(lambda df: union_all(df, select_type1(log_data, types['type1'])))
        .transform(adjust_timestamp_format)
        .transform(adjust_timezone)
        .transform(split_timestamp)
        .transform(remove_comma)
        .transform(remove_quote)
        .transform(explode_list)
        .transform(lambda df: join_dfs(df, category_data))
    )


def select_default(df, *default):
    """Select fields for default log format; 155138, 154992, 4550

    :param df: Input DataFrame
    :param default: A list of shopping_sites_id
    :return: Output DataFrame
    """
    return (
        df
        .filter(df.logtype.isin('login', 'purchase') & df.info.siteseq.isin(*default))
        .select('maid', 'info.siteseq', 'userid', 'custid', 'timestamp', 'logtype',
                json_tuple(df.custom, 'goodsCode', 'goodsName').alias('productCode', 'productName'))
        .withColumnRenamed('info.siteseq', 'siteseq')
        .unionAll((
            df
            .filter(df.logtype.isin('cart') & df.info.siteseq.isin(*default))
            .select('maid', 'info.siteseq', 'userid', 'custid', 'timestamp', 'logtype',
                    json_tuple(df.custom, 'goodsCode', 'name').alias('productCode', 'productName'))
            .withColumnRenamed('info.siteseq', 'siteseq')))
        .unionAll((
            df
            .filter(df.logtype.isin('view') & df.info.siteseq.isin(*default))
            .select('maid', 'info.siteseq', 'userid', 'custid', 'timestamp', 'logtype',
                    json_tuple(df.custom, 'rb:productCode', 'tas:productName').alias('productCode', 'productName'))
            .withColumnRenamed('info.siteseq', 'siteseq'))))


def select_type1(df, *type1):
    """Select fields for default log format; -48

    :param df: Input DataFrame
    :param type1: A list of shopping_sites_id
    :return: Output DataFrame
    """
    return (
        df
        .filter(df.logtype.isin('login', 'purchase') & df.info.siteseq.isin(*type1))
        .select('maid', 'info.siteseq', 'userid', 'custid', 'timestamp', 'logtype',
                json_tuple(df.custom, 'goodsCode', 'goodsName').alias('productCode', 'productName'))
        .withColumnRenamed('info.siteseq', 'siteseq')
        .unionAll((
            df
            .filter(df.logtype.isin('cart') & df.info.siteseq.isin(*type1))
            .select('maid', 'info.siteseq', 'userid', 'custid', 'timestamp', 'logtype',
                    json_tuple(df.custom, 'goodsCode', 'name').alias('productCode', 'productName'))
            .withColumnRenamed('info.siteseq', 'siteseq')))
        .unionAll((
            df
            .filter(df.logtype.isin('view') & df.info.siteseq.isin(*type1))
            .select('maid', 'info.siteseq', 'userid', 'custid', 'timestamp', 'logtype',
                    json_tuple(df.custom, 'rb:productCode', 'tas:productName').alias('productCode', 'productName'))
            .withColumnRenamed('info.siteseq', 'siteseq'))))


def union_all(df1, df2):
    """Union All two DataFrames

    :param df1: Input DataFrame
    :param df2: Input DataFrame
    :return: Output DataFrame
    """
    return (
        df1
        .unionAll(df2)
    )


def adjust_timestamp_format(df):
    """Adjust timestamp format so that all can have the same format

    :param df: Input DataFrame
    :return: Output DataFrame
    """
    return (
        df
        .withColumn('timestamp', regexp_replace(df.timestamp, r'(\d+-\d+-\d+T\d+:\d+:\d+)Z', '$1.000Z')))


def adjust_timezone(df):
    """Adjust timezone to KST from UTC

    :param df: Input DataFrame
    :return: Output DataFrame
    """
    return (
        df
        .withColumn('timestamp', from_utc_timestamp(unix_timestamp(df['timestamp'], "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
                                                    .cast('timestamp'), 'KST')))


def split_timestamp(df):
    """Split timestamp to transaction_time and transaction_date

    :param df: Input DataFrame
    :return: Output DataFrame
    """
    stage_df = df.withColumn('split_timestamp', split(df['timestamp'], ' '))
    return (
        stage_df
        .select('maid', 'siteseq', 'userid', 'custid', 'logtype', stage_df['split_timestamp']
                .getItem(0).alias('transaction_date'), stage_df['split_timestamp']
                .getItem(1).alias('transaction_time'), 'productCode', 'productName'))


def remove_comma(df):
    """Remove any comma in string within column

    :param df: Input DataFrame
    :return: Output DataFrame
    """
    return (
        df
        .withColumn('productCode', regexp_replace(df.productCode, r"[^\"](\,+)|(\,+)[^\"]", ""))
        .withColumn('productName', regexp_replace(df.productName, r"[^\"](\,+)|(\,+)[^\"]", ""))
    )


def remove_quote(df):
    """Remove any quotation marks in string within column

    :param df: Input DataFrame
    :return: Output DataFrame
    """
    return (
        df
        .withColumn('productCode', split(regexp_replace(df.productCode, r"(^\[)|(\]$)|(\")", ""), ","))
        .withColumn('productName', split(regexp_replace(df.productName, r"(^\[)|(\]$)|(\")", ""), ","))
    )


def explode_list(df):
    """Explode a list of products into separated rows

    :param df: Input DataFrame
    :return: Output DataFrame
    """
    return (
        df
        .withColumn('tmp', arrays_zip('productCode', 'productName'))
        .withColumn('tmp', explode_outer('tmp'))
        .select('maid', 'siteseq', 'userid', 'custid', 'transaction_date', 'transaction_time',
                'logtype', 'tmp.productCode', 'tmp.productName')
        .withColumnRenamed('tmp.productCode', 'productCode')
        .withColumnRenamed('tmp.productName', 'productName')
    )


def join_dfs(df1, df2):
    """Join two dfs on keys and select fields needed

    :param df1: log_data DataFrame
    :param df2: category_info DataFrame
    :return: final result DataFrame
    """
    stage_df = df1.join(df2, (df1.productCode == df2.item_code) & (df1.siteseq == df2.shopping_sites_id))
    return (
        stage_df
        .select('maid', 'userid', 'custid', 'siteseq', 'transaction_date', 'transaction_time', 'logtype',
                'item_code', 'item_name', 'category_name1', 'category_name2', 'category_name3', 'category_name4',
                'intg_cat1', 'intg_cat2', 'intg_cat3', 'intg_cat4', 'intg_id')
        .withColumnRenamed('maid', 'ma_id')
        .withColumnRenamed('userid', 'user_id')
        .withColumnRenamed('custid', 'cust_id')
        .withColumnRenamed('siteseq', 'shopping_sites_id')
        .withColumnRenamed('logtype', 'log_type'))


def write_data(df, save_path):
    """Collect data locally and write to CSV.

    :param df: DataFrame to print.
    :param save_path: A Path to save files
    :return: None
    """
    (df
     .coalesce(1)
     .write
     .option('encoding', 'utf-8')
     .csv(save_path, mode='overwrite', header=True))
    return None


def save_hdfs(df, save_path):
    """Collect data and save to HDFS as parquet format

    :param df: DataFrame to save
    :param save_path: A HDFS path to save files
    :return: None
    """
    (
        df
        .write
        .save(save_path, format='parquet', mode='append'))
    return None


def create_test_data(spark, config):
    """Create test data.

    This function creates both both pre- and post- transformation data
    saved as Parquet files in tests/test_data. This will be used for
    unit tests as well as to load as part of the example ETL job.
    :return: None
    """
    # create example data from scratch
    local_records = [
        Row()
    ]


    local_records = [
        Row(id=1, first_name='Dan', second_name='Germain', floor=1),
        Row(id=2, first_name='Dan', second_name='Sommerville', floor=1),
        Row(id=3, first_name='Alex', second_name='Ioannides', floor=2),
        Row(id=4, first_name='Ken', second_name='Lai', floor=2),
        Row(id=5, first_name='Stu', second_name='White', floor=3),
        Row(id=6, first_name='Mark', second_name='Sweeting', floor=3),
        Row(id=7, first_name='Phil', second_name='Bird', floor=4),
        Row(id=8, first_name='Kim', second_name='Suter', floor=4)
    ]

    df = spark.createDataFrame(local_records)

    # write to Parquet file format
    (df
     .coalesce(1)
     .write
     .parquet('tests/test_data/employees', mode='overwrite'))

    # create transformed version of data
    df_tf = transform_data(df, config['steps_per_floor'])

    # write transformed version of data to Parquet
    (df_tf
     .coalesce(1)
     .write
     .parquet('tests/test_data/employees_report', mode='overwrite'))

    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
