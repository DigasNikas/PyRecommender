from pyspark import SQLContext
from pyspark.sql.functions import lit
from datetime import datetime


def prepare_data(sc, months, output_path):

    sqlContext = SQLContext(sc)
    sqlContext.setConf('spark.sql.parquet.compression.codec', 'snappy')

    blacklist = []
    blacklist_top50 = ['({})|'.format(x) for x in get_top50()]
    blacklist_filters = ['(.+\.{}.*)|'.format(x) for x in get_blackList()]
    blacklist.extend(blacklist_top50)
    blacklist.extend(blacklist_filters)
    blacklist = list(set(blacklist))
    rx = ''.join(blacklist)
    rx = rx[:-1]

    # gets all user installs from the selected number of previous months excluding the current month
    df = get_files_from_s3(sqlContext, months)

    # select only the hash and explode the list of packages
    df_pkg = df.select(
        df['hash'].alias('hash'),
        df['pkg'].alias('package')
    ).drop_duplicates().cache()

    # remove incoherent packages like "android"
    rpkg = '.+\..+'
    df_pkg = df_pkg.filter(df_pkg['package'].rlike(rpkg)).cache()

    # filter blacklist packages and top 50
    df_pkg_nosystemapps = df_pkg.filter(~df_pkg['package'].rlike(rx)).cache()

    # connects to database and filter packages with less than 500 downloads
    df_pkg_nosystemapps = filter_less_500_downloads(sqlContext, df_pkg_nosystemapps).cache()

    def toCSVLine(data):
        name = data[0]
        id = data[1]
        return "{},{}".format(name, id)

    # mapping of hashs and ID used for recommendations
    rdd_hashs = df_pkg_nosystemapps.select(df_pkg_nosystemapps['hash']).distinct().rdd.zipWithUniqueId().map(
        lambda x: (x[0][0], x[1] + 1)).cache()
    df_hashs = sqlContext.createDataFrame(rdd_hashs, ['hash', 'user_id'])
    rdd_hashs = rdd_hashs.map(toCSVLine)
    rdd_hashs.repartition(1).saveAsTextFile(output_path + "/hashs")
    rdd_hashs.unpersist()
    print("user hashs saved")

    # mapping of packages and ID used for recommendations
    rdd_packages = df_pkg_nosystemapps.select(df_pkg_nosystemapps['package']).distinct().rdd.zipWithUniqueId().map(
        lambda x: (x[0][0], x[1]+1)).cache()
    df_packages = sqlContext.createDataFrame(rdd_packages, ['package', 'app_id'])
    rdd_packages = rdd_packages.map(toCSVLine)
    rdd_packages.repartition(1).saveAsTextFile(output_path + "/apps")
    print("apps ID's saved")

    def toCSVLine_2(data):
        app_id = data[0]
        count = data[1]
        quo = data[2]
        return "{},{},{}".format(app_id, count, quo)

    # final dataframe to be sent to recommend engine
    df_data = df_pkg_nosystemapps.join(df_hashs, 'hash', 'left_outer').select('user_id', 'package').cache()
    df_data = df_data.join(df_packages, 'package', 'left_outer').select('user_id', 'app_id').cache()
    df_data = df_data.withColumn("rating", lit(1)).cache()
    df_data.rdd.map(toCSVLine_2).repartition(1).saveAsTextFile(output_path + "/dataset")
    print("dataset saved")

    # save apps histogram
    df_hist = get_app_histogram(df_data, df_packages)
    df_hist.rdd.map(toCSVLine_2).repartition(1).saveAsTextFile(output_path + "/histogram")
    print("apps histogram saved")

    return df_data.rdd


def get_files_from_s3(sqlContext, amount_months):

    year = datetime.today().year
    month = datetime.today().month
    if month - amount_months >= 0:
        months = range(month - amount_months, month)
        year_and_month = ["year={}/month={}".format(year, m) for m in months]
    else:
        previous_year_months = [x for x in range(12 - abs(month - amount_months), 13)]
        this_year_months = [x for x in range(1, month)]
        year_and_month = ["year={}/month={}".format(year - 1, m) for m in previous_year_months]
        year_and_month = year_and_month + ["year={}/month={}".format(year, m) for m in this_year_months]
    day = '*'
    filename = '*'
    version = '1'
    filepath = ['{}/{}/{}/{}'.format(version, pair, day, filename) for pair in year_and_month]

    print("reading {}".format(filepath))
    return sqlContext.read.parquet(*filepath)


def filter_less_500_downloads(sqlContext, df_pkg_nosystemapps):

    u, p = ['user', 'password']
    durl = 'url'
    dbta = 'table'
    psql_df = sqlContext.read.format('jdbc').options(url=durl,
                                                     user=u,
                                                     password=p,
                                                     dbtable=dbta,
                                                     driver='org.postgresql.Driver').load()
    psql_df = psql_df.drop(psql_df['added_timestamp'])

    df_pkg_nosystemapps = df_pkg_nosystemapps.join(psql_df,
                                                   psql_df['data'] == df_pkg_nosystemapps['package']) \
        .drop(psql_df['data'])

    dbta = 'table'
    psql_df = sqlContext.read.format('jdbc').options(url=durl,
                                                     user=u,
                                                     password=p,
                                                     dbtable=dbta,
                                                     driver='org.postgresql.Driver').load()
    psql_df = psql_df.drop(psql_df['id'])

    df_pkg_nosystemapps = df_pkg_nosystemapps.join(psql_df,
                                                   psql_df['app_package'] == df_pkg_nosystemapps['id']) \
        .drop(psql_df['app_package']) \
        .drop(df_pkg_nosystemapps['id'])
    df_pkg_nosystemapps = df_pkg_nosystemapps.filter(
        df_pkg_nosystemapps['downloads'] > 500).drop(df_pkg_nosystemapps['downloads'])

    df_pkg_nosystemapps = df_pkg_nosystemapps.drop_duplicates()

    return df_pkg_nosystemapps


def get_app_histogram(df_data, df_packages):
    total = df_data.count()
    df_hist = df_data.groupBy("app_id").count()  # histogram
    df_hist = df_hist.withColumn("total", lit(total))
    df_hist = df_hist.withColumn('percentage', (df_hist['count'] / df_hist['total'])*100)
    df_hist = df_hist.join(df_packages, 'app_id', 'left_outer').select('package', 'count', 'percentage')
    return df_hist


def get_blackList():

    blacklist_filters = ['list']

    return blacklist_filters


def get_top50():

    blacklist_top50 = ['list']

    return blacklist_top50
