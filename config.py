from pyspark import sql, SparkConf, SparkContext

conf = SparkConf().setAppName("app_task")
sc = SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)

data_file = 'temperature_city.csv'
