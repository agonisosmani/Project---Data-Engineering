import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_keyspace(session, keyspace='spark_streams', replication_factor=1):
    """Creates a keyspace in Cassandra if it does not already exist."""
    try:
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '{replication_factor}'}};
        """)
        logging.info("Keyspace created successfully.")
    except Exception as e:
        logging.error(f"Failed to create keyspace: {e}")

def create_table(session, keyspace='spark_streams', table_name='created_users'):
    """Creates a table in the specified keyspace if it does not already exist."""
    try:
        session.execute(f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
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
            picture TEXT
        );
        """)
        logging.info("Table created successfully.")
    except Exception as e:
        logging.error(f"Failed to create table: {e}")

def insert_data(session, keyspace='spark_streams', table_name='created_users', **kwargs):
    """Inserts a record into the specified table."""
    try:
        query = f"""
            INSERT INTO {keyspace}.{table_name} (id, first_name, last_name, gender, address, 
                post_code, email, username, registered_date, phone, picture)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            kwargs.get('id'), kwargs.get('first_name'), kwargs.get('last_name'),
            kwargs.get('gender'), kwargs.get('address'), kwargs.get('post_code'),
            kwargs.get('email'), kwargs.get('username'), kwargs.get('registered_date'),
            kwargs.get('phone'), kwargs.get('picture')
        )
        session.execute(query, params)
        logging.info(f"Data inserted for {kwargs.get('first_name')} {kwargs.get('last_name')}.")
    except Exception as e:
        logging.error(f"Failed to insert data: {e}")

def create_spark_session(app_name='SparkDataStreaming', cassandra_host='localhost'):
    """Creates a Spark session configured for connecting to Cassandra and Kafka."""
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', cassandra_host) \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logging.error(f"Failed to create Spark session: {e}")
        return None

def connect_to_kafka(spark, kafka_servers='localhost:9092', topic='users_created'):
    """Reads data from Kafka into a DataFrame."""
    try:
        kafka_df = spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', kafka_servers) \
            .option('subscribe', topic) \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka DataFrame created successfully.")
        return kafka_df
    except Exception as e:
        logging.warning(f"Failed to connect to Kafka: {e}")
        return None

def create_cassandra_connection(contact_points=['localhost']):
    """Creates a Cassandra session for executing CQL commands."""
    try:
        cluster = Cluster(contact_points)
        session = cluster.connect()
        logging.info("Cassandra connection established successfully.")
        return session
    except Exception as e:
        logging.error(f"Failed to connect to Cassandra: {e}")
        return None

def create_selection_df(spark_df):
    """Parses the Kafka DataFrame to extract and select fields based on a predefined schema."""
    schema = StructType([
        StructField("id", StringType(), False),
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
    try:
        parsed_df = spark_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col('value'), schema).alias('data')) \
            .select("data.*")
        logging.info("Selection DataFrame created successfully.")
        return parsed_df
    except Exception as e:
        logging.error(f"Failed to create selection DataFrame: {e}")
        return None

def main():
    """Main function to set up Spark streaming and insert data into Cassandra."""
    spark = create_spark_session()
    if not spark:
        return

    kafka_df = connect_to_kafka(spark)
    if kafka_df is None:
        return

    selection_df = create_selection_df(kafka_df)
    if selection_df is None:
        return

    cassandra_session = create_cassandra_connection()
    if cassandra_session:
        create_keyspace(cassandra_session)
        create_table(cassandra_session)

        logging.info("Starting streaming query...")
        streaming_query = (
            selection_df.writeStream
            .format("org.apache.spark.sql.cassandra")
            .option('checkpointLocation', '/tmp/checkpoint')
            .option('keyspace', 'spark_streams')
            .option('table', 'created_users')
            .start()
        )

        streaming_query.awaitTermination()

if __name__ == "__main__":
    main()
