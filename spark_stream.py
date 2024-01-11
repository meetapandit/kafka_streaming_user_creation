import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid

def create_keyspace(session):
    # create cassandra keyspace
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class' : 'SimpleStrategy', 'replication_factor': '1'}
        """
    )

    print("Keyspace created successfully!")

def create_table(session):
    # create table in cassandra keyspace
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)

    print("Table created successfully!")

def insert_data(session, **kwargs):
    print("inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")
        print("data insersted successfully!")
    except Exception as e:
        logging.error(f'could not insert data due to {e}')

def create_spark_connection():
    spark = None
    print(type(spark))
    try:
        print("running the try block")
        spark = SparkSession.builder \
                            .appName("SparkdataStreaming") \
                            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
                            .config('spark.cassandra.connection.host', 'localhost') \
                            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")

    except Exception as e:
        logging.error(f'Couldn\'t create spark connection due to exception {e}')
    print(type(spark))
    return spark
        
def connect_to_kafka(spark_conn):
    # read data from kafka using spark session
    spark_df = None
    try:
        print("try statement from connect to kafka")
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .option("failOnDataLoss", "false") \
            .load()
        logging.info("kafka dataframe created successfully")
        print("finished reading from kafka")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")
    print("type spark_df in connect_to_kafka", type(spark_df))
    return spark_df

def create_selection_df_from_kafka(df):
    schema = StructType([
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col('value'), schema).alias('data')).select("data.*")
    
    uuidUdf = udf(lambda : str(uuid.uuid4()),StringType())
    spark_df = sel.withColumn("id",uuidUdf())

    return spark_df

def create_cassandra_connection():
    print("inside create_cassandra_session")
    try:
        # creating to the cassandra cluster
        cluster = Cluster(['localhost'])
        cass_session = cluster.connect()

        return cass_session
    
    except Exception as e:
        logging.error(f'Could not create cassandra connection due to {e}')
        return None


if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        df = connect_to_kafka(spark_conn)
        # print("df", df)
        selection_df = create_selection_df_from_kafka(df)
        # print(df.show())
        session = create_cassandra_connection()
        print("cassandra session", session)
        if session is not None:
            print("before calling keyspace creation")
            create_keyspace(session)
            create_table(session) 
            # insert_data(session)

            logging.info("Streaming is being started...")

            streaming_query = selection_df.writeStream.format("org.apache.spark.sql.cassandra") \
                               .option('checkpointLocation', '/tmp/checkpoint') \
                               .option('keyspace', 'spark_streams') \
                               .option('table', 'created_users') \
                               .start()

            streaming_query.awaitTermination()