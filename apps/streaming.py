from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
spark = SparkSession.builder.appName("streaming")\
    .config("spark.sql.shuffle.partitions", "4")\
    .config("spark.streaming.StopGracefullyOnShutdown", "true")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
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
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("customer_name", StringType()),
    StructField("customer_email", StringType()),
    StructField("items", ArrayType(StructType([
        StructField("product_id", StringType()),
        StructField("quantity", IntegerType()),
        StructField("price", IntegerType())
    ]))),
    StructField("total_amount", DoubleType()),
    StructField("payment_type", StringType()),
    StructField("currency", StringType()),
    StructField("order_status", ArrayType(StructType([
        StructField("order_status", StringType()),
        StructField("payment_status", StringType()),
        StructField("status_timestamp", StringType())
    ]))),
    StructField("ingestion_timestamp", StringType())
])

payment_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("payment_id", StringType()),
    StructField("payment_method", StringType()),
    StructField("payment_provider", StringType()),
    StructField("card_network", StringType()),
    StructField("issuing_bank", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("payment_type", StringType()),
    StructField("ingestion_timestamp", StringType())
])

return_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("return_id", StringType()),
    StructField("return_reason", StringType()),
    StructField("return_status", StringType()),
    StructField("refund_amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("ingestion_timestamp", StringType())
])

df_order=df_order.select(from_json(col("value"), order_schema).alias("data")).select("data.*")
df_payment=df_payment.select(from_json(col("value"), payment_schema).alias("data")).select("data.*")
df_return=df_return.select(from_json(col("value"), return_schema).alias("data")).select("data.*")


df_order=df_order.select("event_id", "event_type", "event_timestamp", "order_id", "customer_id",\
                   "customer_name", "customer_email", "items", "total_amount", "payment_type",\
                       "currency", "order_status", "ingestion_timestamp")

df_payment=df_payment.select("event_id", "event_type", "event_timestamp", "order_id", "customer_id",\
                   "payment_id", "payment_method", "payment_provider", "card_network",\
                    "issuing_bank", "amount", "currency", "payment_type","ingestion_timestamp")

df_return=df_return.select("event_id", "event_type", "event_timestamp", "order_id", "customer_id",\
                   "return_id", "return_reason", "return_status", "refund_amount", "currency", "ingestion_timestamp")

def process_order_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
  # Table 1: order_items
    items_df = batch_df.withColumn("item", explode_outer("items"))\
        .select("event_id", "order_id", "customer_id", "customer_name",
                "customer_email", "total_amount", "payment_type", "currency",
                "event_timestamp", "ingestion_timestamp",
                "item.product_id", "item.quantity", "item.price")
    items_df.write.format("delta").mode("append")\
        .save("s3a://ecommerce-spark-streaming/streaming/order_items/")

    # Table 2: order_status
    status_df = batch_df.withColumn("status", explode_outer("order_status"))\
        .select("event_id", "order_id", "customer_id",
                "event_timestamp", "ingestion_timestamp",
                "status.order_status", "status.payment_status",
                "status.status_timestamp")
    status_df.write.format("delta").mode("append")\
        .save("s3a://ecommerce-spark-streaming/streaming/order_status/")
    
df_order.writeStream.foreachBatch(process_order_batch)\
       .option("checkpointLocation", "s3a://ecommerce-spark-streaming/streaming/checkpoint/orders/")\
           .trigger(processingTime="5 seconds")\
                .start()
         
df_payment.writeStream.format("delta")\
       .option("checkpointLocation", "s3a://ecommerce-spark-streaming/streaming/checkpoint/payments/")\
           .option("path", "s3a://ecommerce-spark-streaming/streaming/payments/")\
                .trigger(processingTime="5 seconds")\
                .start()
         
df_return.writeStream.format("delta")\
       .option("checkpointLocation", "s3a://ecommerce-spark-streaming/streaming/checkpoint/returns/")\
           .option("path", "s3a://ecommerce-spark-streaming/streaming/returns/")\
                .trigger(processingTime="5 seconds")\
                .start()
                
spark.streams.awaitAnyTermination()
                
    
    

    
                              