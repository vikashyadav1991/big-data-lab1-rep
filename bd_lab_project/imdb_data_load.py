from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.functions import *
import pyspark_cassandra, sys

def trim(string):
    return string.strip()

def readDataFrame(sqlContext, schema, inputs):
    # Reading the movie_metadata.csv file and triming the whitespace
    df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(inputs + '/movie_metadata.csv')
    df_trim = df.select(trim('color').alias('color'), trim('director_name').alias('director_name'), 'num_critic_for_reviews', 'duration',
                    'director_facebook_likes', 'actor_3_facebook_likes', trim('actor_2_name').alias('actor_2_name'), 'actor_1_facebook_likes',
                    'gross', trim('genres').alias('genres'), trim('actor_1_name').alias('actor_1_name'), trim('movie_title').alias('movie_title'),
                    'num_voted_users', 'cast_total_facebook_likes', trim('actor_3_name').alias('actor_3_name'), trim('plot_keywords').alias('plot_keywords'),
                    trim('movie_imdb_link').alias('movie_imdb_link'), 'num_user_for_reviews', trim('language').alias('language'),
                    trim('country').alias('country'), trim('content_rating').alias('content_rating'), 'budget', 'title_year', 'actor_2_facebook_likes',
                    'imdb_score', 'aspect_ratio', 'movie_facebook_likes').cache()
    df_trim = df_trim.na.fill({'duration': 98, 'gross': 0, 'aspect_ratio': 2.35, 'actor_1_facebook_likes':0,
                                  'num_critic_for_reviews':0, 'title_year': 2001,'num_user_for_reviews':0, 'actor_2_facebook_likes': 0,
                                  'actor_3_facebook_likes': 0, 'director_facebook_likes': 0, 'content_rating': 'Not Rated'})
    df_trim.write.format("org.apache.spark.sql.cassandra").option("table", "imdb_movie_data").option("keyspace",schema).save()


def main(sqlContext, schema, inputs):
    readDataFrame(sqlContext, schema, inputs)

if __name__ == "__main__":
    cluster_seeds = ['199.60.17.136', '199.60.17.173']
    conf = SparkConf().set('spark.cassandra.connection.host', ','.join(cluster_seeds)).set('spark.dynamicAllocation.maxExecutors', 20)
    sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    trim = udf(trim)
    schema = sys.argv[1]
    inputs = sys.argv[2]
    main(sqlContext, schema, inputs)