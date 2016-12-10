from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.functions import mean
from pyspark.sql.types import IntegerType, StringType
import pyspark_cassandra, sys

def readDataFrame(sqlContext, schema, inputs):

    # UDF for parsing rating
    get_rating = udf(lambda link: int(round(link)), IntegerType())
    # UDF for parsing imdb url for the movie
    get_link = udf(lambda link: "http://www.imdb.com/title/tt" + str(link).zfill(7) + "/?ref_=fn_tt_tt_1", StringType())

    # Read csv files into dataframe
    movie_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")\
        .option("inferSchema", "true").load(inputs + '/movies.csv').selectExpr('movieId as movieid','title', 'genres').registerTempTable("movie")
    rating_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")\
        .option("inferSchema","true").load(inputs + '/ratings.csv').selectExpr('movieId as movieid', 'rating').cache()
    rating_df = rating_df.groupBy('movieid').agg(mean("rating").alias("rating")).select('movieid', get_rating('rating').alias('rating')).registerTempTable("rating")
    imdblink_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")\
        .option("inferSchema","true").load(inputs + '/links.csv').registerTempTable("imdblink")

    # Joining Data to load in cassandra
    movie_lens_df = sqlContext.sql("""select movie.movieid, movie.title, rating.rating, movie.genres, imdblink.imdbid
    from movie join rating on (movie.movieid = rating.movieid) join imdblink on (movie.movieid = imdblink.movieid)""")

    # Saving imdb url in a json file, File contains appx. 40K records and is of 2.6MB in size
    movie_lens_df.select(get_link('imdbid').alias('imdbid')).coalesce(1).write.save('imdblinks', format='json', mode='overwrite')
    # Saving data to cassandra
    movie_lens_df.write.format("org.apache.spark.sql.cassandra").option("table", "movie_lens_data").option("keyspace",schema).save()


def main(sqlContext, schema, inputs):
    readDataFrame(sqlContext, schema, inputs)

if __name__ == "__main__":
    cluster_seeds = ['199.60.17.136', '199.60.17.173']
    conf = SparkConf().set('spark.cassandra.connection.host', ','.join(cluster_seeds)).set('spark.dynamicAllocation.maxExecutors', 20)
    sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    schema = sys.argv[1]
    inputs = sys.argv[2]
    main(sqlContext, schema, inputs)