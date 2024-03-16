from pyspark.sql import SparkSession
from dataframe_utils.transform import generate_marketing_push_data
import logging
from log import configure_logger


def main():
    # creating a spark session
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KommatiPara Solution") \
        .getOrCreate()

    # reading input datasets into the dataframes
    client_input_df = spark.read.csv("source_data/dataset_one.csv", header=True)
    financial_input_df = spark.read.csv("source_data/dataset_two.csv", header=True)

    # generating the data for marketing push
    final_df = generate_marketing_push_data(client_input_df, financial_input_df, 'United Kingdom')

    # writing data in csv format
    write_path = "client_data"
    final_df.write.mode("overwrite").option("header", "true").csv(write_path)
    logging.info("Result is successfully written in the output path %s", write_path)


if __name__ == "__main__":
    configure_logger("app.log")
    main()
