from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
spark=SparkSession.builder.appName("streaming")\
    .config("spark.sql.shuffle.partitions", "4")\
        .config("spark.streaming.StopGracefullyOnShutdown", "true")\
            .master("spark://spark-master:7077")\
                .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))\
                .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))\
                     .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")\
            .getOrCreate()

df_order_stream=spark.readStream.format("kafka")\
            .option("kafka.bootstrap.servers", "kafka:9092")\
            .option("subscribe", "order_event")\
            .option("startingOffsets", "latest")\
            .load()
            
df_payment_stream=spark.readStream.format("kafka")\
            .option("kafka.bootstrap.servers", "kafka:9092")\
            .option("subscribe", "payment_event")\
            .option("startingOffsets", "latest")\
            .load()
        
df_return_stream=spark.readStream.format("kafka")\
            .option("kafka.bootstrap.servers", "kafka:9092")\
            .option("subscribe", "return_event")\
            .option("startingOffsets", "latest")\
            .load()
            
df_order=df_order_stream.select(col('value').cast('string'))
df_payment=df_payment_stream.select(col('value').cast('string'))
df_return=df_return_stream.select(col('value').cast('string'))

order_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),

    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_email", StringType(), True),

    StructField("items", ArrayType(
        StructType([
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", IntegerType(), True)
        ])
    ), True),

    StructField("total_amount", DoubleType(), True),
    StructField("payment_type", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True)
])

payment_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),

    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),

    StructField("payment_id", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_provider", StringType(), True),
    StructField("card_network", StringType(), True),
    StructField("issuing_bank", StringType(), True),

    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),

    StructField("payment_type", StringType(), True),
    StructField("payment_status", StringType(), True),

    StructField("ingestion_timestamp", StringType(), True)
])

return_schema= StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),

    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),

    StructField("return_id", StringType(), True),
    StructField("return_reason", StringType(), True),
    StructField("return_status", StringType(), True),

    StructField("refund_amount", DoubleType(), True),
    StructField("currency", StringType(), True),

    StructField("ingestion_timestamp", StringType(), True)
])

df_order=df_order.select(from_json(col("value"), order_schema).alias("data")).select("data.*")
df_payment=df_payment.select(from_json(col("value"), payment_schema).alias("data")).select("data.*")
df_return=df_return.select(from_json(col("value"), return_schema).alias("data")).select("data.*")


df_order=df_order.select("event_id", "event_type", "event_timestamp", "order_id", "customer_id",\
                   "customer_name", "customer_email", "items", "total_amount", "payment_type",\
                       "currency", "order_status", "ingestion_timestamp")

df_payment=df_payment.select("event_id", "event_type", "event_timestamp", "order_id", "customer_id",\
                   "payment_id", "payment_method", "payment_provider", "card_network",\
                    "issuing_bank", "amount", "currency", "payment_type", "payment_status","ingestion_timestamp")

df_return=df_return.select("event_id", "event_type", "event_timestamp", "order_id", "customer_id",\
                   "return_id", "return_reason", "return_status", "refund_amount", "currency", "ingestion_timestamp")

def process_order_batch(batch_df, batch_id):
    batch_df=batch_df.withColumn("item",explode_outer("items"))
    batch_df=batch_df.select("event_id", "event_type", "event_timestamp", "order_id", "customer_id",\
                   "customer_name", "customer_email", "item.product_id", "item.quantity",\
                          "item.price", "total_amount", "payment_type","currency", "order_status", "ingestion_timestamp")
    batch_df.write.format("parquet").mode("append").save("s3a://ecommerce-spark-streaming/streaming/orders/")
    
df_order.writeStream.foreachBatch(process_order_batch)\
       .option("checkpointLocation", "s3a://ecommerce-spark-streaming/streaming/checkpoint/orders/")\
           .trigger(processingTime="2 seconds")\
                .start()
         
df_payment.writeStream.format("parquet")\
       .option("checkpointLocation", "s3a://ecommerce-spark-streaming/streaming/checkpoint/payments/")\
           .option("path", "s3a://ecommerce-spark-streaming/streaming/payments/")\
                .trigger(processingTime="2 seconds")\
                .start()
         
df_return.writeStream.format("parquet")\
       .option("checkpointLocation", "s3a://ecommerce-spark-streaming/streaming/checkpoint/returns/")\
           .option("path", "s3a://ecommerce-spark-streaming/streaming/returns/")\
                .trigger(processingTime="2 seconds")\
                .start()
                
spark.streams.awaitAnyTermination()
                
    
    

    
                              