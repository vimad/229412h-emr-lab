import argparse

from pyspark.sql import SparkSession

def calculate_year_wise_late_air_craft_delay(data_source, output_uri):
    with SparkSession.builder.appName("Calculate Year Wise Late Air Craft Delay").getOrCreate() as spark:
        if data_source is not None:
            flights_df = spark.read.option("header", "true").csv(data_source)
        flights_df.createOrReplaceTempView("delayed_flights")


        year_wise_late_air_craft_delay = spark.sql("""SELECT Year,
            AVG((CASE WHEN ArrDelay = 0 THEN 0 ELSE LateAircraftDelay / ArrDelay END) * 100) AS AvgLateAircraftDelayPercentage
            FROM delayed_flights
            WHERE Year >= 2003 AND Year <= 2010 GROUP BY Year""")

        # Write the results to the specified output URI
        year_wise_late_air_craft_delay.write.option("header", "true").mode("overwrite").csv(output_uri)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="The URI for you CSV restaurant data, like an S3 bucket location.")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, like an S3 bucket location.")
    args = parser.parse_args()

    calculate_year_wise_late_air_craft_delay(args.data_source, args.output_uri)