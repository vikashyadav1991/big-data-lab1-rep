from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.mllib.tree import DecisionTree, RandomForest as RF
from pyspark.mllib.classification import NaiveBayes
from time import time
from pyspark.mllib.regression import LabeledPoint
from numpy import array
import pyspark_cassandra, sys, re

def create_labeled_point(line_split, director, actor_1, language, content_rating, genres):
    # leave_out = [7]
    clean_line_split = line_split[0:7]
    # convert protocol to numeric categorical variable
    try:
        clean_line_split[0] = director.index(clean_line_split[0])
    except:
        clean_line_split[0] = len(director)
    try:
        clean_line_split[1] = actor_1.index(clean_line_split[1])
    except:
        clean_line_split[1] = len(actor_1)
    try:
        clean_line_split[2] = language.index(clean_line_split[2])
    except:
        clean_line_split[2] = len(language)
    try:
        clean_line_split[3] = content_rating.index(clean_line_split[3])
    except:
        clean_line_split[3] = len(content_rating)
    try:
        clean_line_split[4] = genres.index(clean_line_split[4])
    except:
        clean_line_split[4] = len(genres)
    # convert service to numeric categorical variable
    return LabeledPoint(line_split[7], array([int(x) for x in clean_line_split]))

def create_labeled_point_nb(line_split, director, actor_1, language, content_rating, genres):
    # leave_out = [5]
    clean_line_split = line_split[0:5]
    # convert protocol to numeric categorical variable
    try:
        clean_line_split[0] = director.index(clean_line_split[0])
    except:
        clean_line_split[0] = len(director)
    try:
        clean_line_split[1] = actor_1.index(clean_line_split[1])
    except:
        clean_line_split[1] = len(actor_1)
    try:
        clean_line_split[2] = language.index(clean_line_split[2])
    except:
        clean_line_split[2] = len(language)
    try:
        clean_line_split[3] = content_rating.index(clean_line_split[3])
    except:
        clean_line_split[3] = len(content_rating)
    try:
        clean_line_split[4] = genres.index(clean_line_split[4])
    except:
        clean_line_split[4] = len(genres)
    # convert service to numeric categorical variable
    return LabeledPoint(line_split[5], array([int(x) for x in clean_line_split]))

def create_labeled_point_test(line_split, director, actor_1, language, content_rating, genres, movie):
    # leave_out = [7]
    clean_line_split = line_split[0:7]
    # convert protocol to numeric categorical variable
    try:
        clean_line_split[0] = director.index(clean_line_split[0])
    except:
        clean_line_split[0] = len(director)
    try:
        clean_line_split[1] = actor_1.index(clean_line_split[1])
    except:
        clean_line_split[1] = len(actor_1)
    try:
        clean_line_split[2] = language.index(clean_line_split[2])
    except:
        clean_line_split[2] = len(language)
    try:
        clean_line_split[3] = content_rating.index(clean_line_split[3])
    except:
        clean_line_split[3] = len(content_rating)
    try:
        clean_line_split[4] = genres.index(clean_line_split[4])
    except:
        clean_line_split[4] = len(genres)
    try:
        line_split[7] = movie.index(line_split[7])
    except:
        line_split[7] = len(movie)
    # convert service to numeric categorical variable
    return LabeledPoint(int(line_split[7]), array([int(x) for x in clean_line_split]))


# Function will create dataframe of selected columns
def df_for_fulltbl(keyspace, table, sqlContext, split_size=None):
    df = sqlContext.createDataFrame(sc.cassandraTable(keyspace, table, split_size=split_size)
                                    .select('director_name','duration','actor_1_name','actor_2_name','language',
                                            'actor_3_name','cast_total_facebook_likes','country','content_rating','budget',
                                            'duration','movie_imdb_link','genres','movie_title').setName(table))
    df.registerTempTable(table)
    return df

# Function will create dataframe of two columns
def df_for_partialtbl(keyspace, table, sqlContext, col1, col2, split_size=None):
    df = sqlContext.createDataFrame(sc.cassandraTable(keyspace, table, split_size=split_size).select(col1, col2).setName(table))
    df.registerTempTable(table)
    return df.cache()

def main(sqlContext, schema):
    tbl_schema = StructType([
        StructField('movie_title', StringType(), False),
        StructField('rating', IntegerType(), False)
    ])
    rex = re.compile("^(.+)/tt(.+)/(.+)$")
    get_imdb = udf(lambda link: int(rex.split(link)[2]), IntegerType())
    df_imdb_movie_data = df_for_fulltbl(schema, 'imdb_movie_data', sqlContext)\
        .select('director_name','actor_1_name','country','content_rating','duration','budget', 'genres', 'language',
                'cast_total_facebook_likes',get_imdb('movie_imdb_link').alias('imdbid')).cache().registerTempTable('imdb_movie_data')
    df_movielens_data = df_for_partialtbl(schema, 'movie_lens_data', sqlContext, 'imdbid','rating').cache()
    df_joined = sqlContext.sql("""select
    b.director_name, b.actor_1_name, b.country,b.content_rating, b.duration,b.budget,b.cast_total_facebook_likes,
    b.genres,b.language, a.rating from movie_lens_data a join imdb_movie_data b on (a.imdbid = b.imdbid)""")

    # load imdb test data
    df_imdb_movie_test_data = df_for_fulltbl(schema, 'imdb_movie_test_data', sqlContext)\
        .select('director_name','actor_1_name','language','content_rating', 'genres','budget','cast_total_facebook_likes', 'movie_title', get_imdb('movie_imdb_link').alias('imdbid'))\
        .cache()

    data = df_joined.select('director_name','actor_1_name','language','content_rating', 'genres','budget','cast_total_facebook_likes','rating').dropna()\
        .rdd.map(lambda l: [l[0],l[1],l[2],l[3],l[4],l[5],l[6],l[7]]).cache()
    test = df_imdb_movie_test_data.select('director_name','actor_1_name','language','content_rating', 'genres','budget','cast_total_facebook_likes','movie_title')\
        .dropna().rdd.map(lambda l: [l[0],l[1],l[2],l[3],l[4],l[5],l[6],l[7]]).cache()

    movie = test.map(lambda x: x[7]).distinct().collect()
    director = data.map(lambda x: x[0]).distinct().collect()
    actor_1 = data.map(lambda x: x[1]).distinct().collect()
    language = data.map(lambda x: x[2]).distinct().collect()
    content_rating = data.map(lambda x: x[3]).distinct().collect()
    genres = data.map(lambda x: x[4]).distinct().collect()

    T_data = data.map(lambda l: create_labeled_point(l, director, actor_1, language, content_rating, genres))
    training, validation = T_data.randomSplit([9, 1], 17)

    # Training decision tree model
    t0 = time()
    tree_model = DecisionTree.trainClassifier(training, numClasses=6, categoricalFeaturesInfo={0: len(director), 1: len(actor_1),
                                                                                               2: len(language), 3: len(content_rating),
                                                                                               4: len(genres)},
                                              impurity='gini', maxDepth=8, maxBins=max(len(director),len(actor_1)))
    tt = time() - t0

    print "Learned Decision tree model:"
    # print tree_model.toDebugString()
    print "Classifier trained in {} seconds".format(round(tt, 3))

    # Testing decision tree model
    predictions = tree_model.predict(validation.map(lambda p: p.features))
    labels_and_preds = validation.map(lambda p: p.label).zip(predictions)

    t0 = time()
    validation_accuracy = labels_and_preds.filter(lambda (v, p): v == p).count() / float(validation.count())
    tt = time() - t0

    print "Prediction made in {} seconds. Decision Tree Validation accuracy is {}".format(round(tt, 3), round(validation_accuracy, 4))

    #Training Random Forest Algo
    rf_model = RF.trainClassifier(training, numClasses=6, categoricalFeaturesInfo={0: len(director), 1: len(actor_1),
                                                                                               2: len(language), 3: len(content_rating),
                                                                                               4: len(genres)},numTrees=6,
                                  featureSubsetStrategy="auto", impurity='gini', maxDepth=8, maxBins=max(len(director),len(actor_1)))
    print "Learned Random Forest tree model:"
    # print rf_model.toDebugString()
    # Testing Random Forest tree model
    predictions = rf_model.predict(validation.map(lambda p: p.features))
    labels_and_preds = validation.map(lambda p: p.label).zip(predictions)

    t0 = time()
    validation_accuracy = labels_and_preds.filter(lambda (v, p): v == p).count() / float(validation.count())
    tt = time() - t0

    print "Prediction made in {} seconds. Random Forest Validation accuracy is {}".format(round(tt, 3), round(validation_accuracy, 4))

    # Naive Bayes Algo
    data = df_joined.select('director_name', 'actor_1_name', 'language', 'content_rating', 'genres', 'rating') \
        .rdd.map(lambda l: [l[0], l[1], l[2], l[3], l[4], l[5]]).cache()

    T_data = data.map(lambda l: create_labeled_point_nb(l, director, actor_1, language, content_rating, genres))
    training, validation = T_data.randomSplit([9, 1], 17)
    t0 = time()
    model = NaiveBayes.train(training)
    tt = time() - t0
    print "Learned Naive Bayes classification model:"
    # print tree_model.toDebugString()
    print "Classifier trained in {} seconds".format(round(tt, 3))
    # Testing Naive Bayes model
    labels_and_preds = validation.map(lambda p: (model.predict(p.features), p.label))
    t0 = time()
    validation_accuracy = labels_and_preds.filter(lambda (v, p): v == p).count() / float(validation.count())
    tt = time() - t0
    print "Prediction made in {} seconds. Naive Bayes Validation accuracy is {}".format(round(tt, 3), round(validation_accuracy, 4))

    # Using Random Forest to Predict Rating for test data
    test_data = test.map(lambda l: create_labeled_point_test(l, director, actor_1, language, content_rating, genres, movie)).cache()
    predict_rating = rf_model.predict(test_data.map(lambda l: l.features))
    test_labels_and_preds = test_data.map(lambda p: movie[int(p.label)]).zip(predict_rating).map(lambda l: (l[0], int(l[1])))
    df=sqlContext.createDataFrame(test_labels_and_preds,tbl_schema)
    df.write.format("org.apache.spark.sql.cassandra").option("table", "imdb_test_ratings").option("keyspace", schema).save()
    print "Prediction Complete ...."
    print "Predicted values loaded into cassandra table imdb_test_ratings"
    print "***********************************************************"
    print df.show()
    print "***********************************************************"


if __name__ == "__main__":
    cluster_seeds = ['199.60.17.136', '199.60.17.173']
    conf = SparkConf().set('spark.cassandra.connection.host', ','.join(cluster_seeds)).set('spark.dynamicAllocation.maxExecutors', 20)
    sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    schema = sys.argv[1]
    main(sqlContext, schema)
