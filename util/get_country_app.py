from pyspark import SparkContext, SparkConf, SQLContext
from prepare_data import get_files_from_s3
import sys

def main(months, output_path):
    conf = SparkConf().setMaster("local[*]").setAppName("AptoideALS")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("OFF")
    sqlContext = SQLContext(sc)
    sqlContext.setConf('spark.sql.parquet.compression.codec', 'snappy')

    df = get_files_from_s3(sqlContext, months)
    df_country = get_app_country(df)

    def toCSVLine_2(data):
        app_id = data[0]
        count = data[1]
        quo = data[2]
        return "{},{},{}".format(app_id, count, quo)

    df_country.rdd.map(toCSVLine_2).repartition(1).saveAsTextFile(output_path + "/country_info")


def get_app_country(sqlContext, df_pkg_nosystemapps):
    df_retention = sqlContext.read.parquet("s3://aptoide-big-data/yellow/retention/*").select('hash', 'country')
    df_app_country = df_pkg_nosystemapps.join(df_retention, 'hash', 'left_outer').select('package', 'country').cache()
    df_country = df_app_country.groupBy(['country']).count().select('country', func.col('count').alias('#per_country'))
    df_app = df_app_country.groupBy(['package']).count().select('package', func.col('count').alias('#per_app'))
    df_app_country = df_app_country.groupBy(['package', 'country']).count().select('package', 'country', func.col('count').alias('#per_pair'))
    df_app_country = df_app_country.drop_duplicates()
    df_app_country = df_app_country.join(df_country, 'country', 'left_outer').join(df_app, 'package', 'left_outer')
    df_app_country = df_app_country.withColumn('quo', df_app_country['#per_pair'] / df_app_country['per_app'])
    return df_app_country

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
