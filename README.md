Download the code on your personal computer including the directory structure. This folder should have below directory structure.

hadoop@vikash-HP-Notebook ~/Desktop/bd_lab_project $ ls -lR
.:
total 650756
-rw-r--r-- 1 hadoop hadoop      2386 Dec  9 21:06 imdb_data_load.py
-rw-r--r-- 1 hadoop hadoop       706 Dec  9 20:08 imdb_movie_data.cql
-rw-r--r-- 1 hadoop hadoop     11340 Dec  9 21:18 imdb_movielens_data.py
-rw-r--r-- 1 hadoop hadoop       562 Dec  9 20:28 imdb_movie_test_data.cql
-rw-r--r-- 1 hadoop hadoop      2318 Dec  9 20:39 imdb_test_data_load.py
-rw-r--r-- 1 hadoop hadoop        85 Dec  9 20:45 imdb_test_ratings.cql
-rwxrwxrwx 1 hadoop hadoop    859311 Nov 21 17:31 links.csv
-rwxrwxrwx 1 hadoop hadoop       128 Dec  9 19:02 movie_lens_data.cql
-rw-r--r-- 1 hadoop hadoop      2520 Dec  9 19:32 movielens_data_load.py
drwxrwxrwx 3 hadoop hadoop      4096 Dec  9 20:01 movie_rating_prediction-master
-rwxrwxrwx 1 hadoop hadoop   2007982 Oct 17 15:39 movies.csv
-rwxrwxrwx 1 hadoop hadoop 663420664 Oct 17 15:28 ratings.csv
-rwxrwxrwx 1 hadoop hadoop      4803 Dec  9 21:20 RUNNING.txt
-rw-r--r-- 1 hadoop hadoop     22190 Dec  9 20:24 test_movie_metadata.csv

./movie_rating_prediction-master:
total 116384
-rwxrwxrwx 1 hadoop hadoop      2061 Dec  9 01:06 ccy_code.csv
-rwxrwxrwx 1 hadoop hadoop   2603238 Dec  7 03:40 imdblinks.json
-rwxrwxrwx 1 hadoop hadoop 105983533 Dec  4 16:08 imdb_output.json
drwxrwxrwx 3 hadoop hadoop      4096 Nov 24 01:14 movie
-rw-r--r-- 1 hadoop hadoop  10555884 Dec  9 20:01 movie_metadata.csv
-rwxrwxrwx 1 hadoop hadoop      9144 Dec  9 01:10 parse_scraped_data.py
-rwxrwxrwx 1 hadoop hadoop       358 Dec  9 19:25 README.md
-rwxrwxrwx 1 hadoop hadoop       254 Aug 29 21:06 scrapy.cfg

./movie_rating_prediction-master/movie:
total 28
-rwxrwxrwx 1 hadoop hadoop  129 Aug 29 21:06 __init__.pyc
-rwxrwxrwx 1 hadoop hadoop 1443 Nov 24 01:12 items.py
-rwxrwxrwx 1 hadoop hadoop 1978 Nov 24 01:14 items.pyc
-rwxrwxrwx 1 hadoop hadoop  420 Aug 29 21:06 pipelines.py
-rwxrwxrwx 1 hadoop hadoop 3208 Aug 29 21:06 settings.py
-rwxrwxrwx 1 hadoop hadoop  475 Nov 23 21:32 settings.pyc
drwxrwxrwx 2 hadoop hadoop 4096 Dec  3 14:25 spiders

./movie_rating_prediction-master/movie/spiders:
total 36
-rwxrwxrwx 1 hadoop hadoop 12921 Nov 24 01:14 imdb_spider.py
-rwxrwxrwx 1 hadoop hadoop  8247 Dec  3 14:25 imdb_spider.pyc
-rwxrwxrwx 1 hadoop hadoop   161 Aug 29 21:06 __init__.py
-rwxrwxrwx 1 hadoop hadoop   166 Dec  3 14:25 __init__.pyc

Step 1) Create a bd_project_data directory in HDFS and place links.csv, movies.csv, ratings.csv in this folder.

Step 2) Create cassandra table using script movie_lens_data.cql to load the data for above three files into cassandra table movie_lens_data.

Step 3) Place the movielens data load script movielens_data_load.py on unix server and execute below command to load the data.

spark-submit --master=yarn-client --packages com.databricks:spark-csv_2.11:1.5.0,TargetHolding/pyspark-cassandra:0.3.5 movielens_data_load.py <cassandra_schema> bd_project_data

this step should load data in movie_lens_data cassandra table and create an output file on HDFS.

hadoop fs -cp imdblinks/part* imdblinks.json

this will create an imdblinks.json file in your HDFS home path.


Step 4) Crawl the data from imdb. In this step we will crawl data from imdb. It takes about 48-72 hours for crawling process to complete.
You can either skip this step and move to step 5 in that case you can reuse already parsed movie_metadata.csv file directly.

Change directory to movie_rating_prediction-master in the downloaded code.
Remove imdblinks.json, imdb_output.json and movie_metadata.csv file for current directory (in case any of it exists).

Move imdblinks.json file created in step 2 in the current movie_rating_prediction-master directory.

change base_dir path in parse_scraped_data.py as per your folder structure.
base_dir = "/home/hadoop/Desktop/movie_rating_prediction-master"

Execute below commands sequentially -

scrapy crawl imdb -o imdb_output.json

Above command should create a imdb_output.json file in current directory.

python parse_scraped_data.py

Above command should output movie_metadata.csv. Place this file on hadoop cluster in bd_project_data.

Step 5) Place movie_metadata.csv file on hadoop cluster in bd_project_data folder.
Create a cassandra table imdb_movie_data to load this file in cassandra using script in imdb_movie_data.cql
Place the imdb_data_load.py on the unix and execute below command.

spark-submit --master=yarn-client --packages com.databricks:spark-csv_2.11:1.5.0,TargetHolding/pyspark-cassandra:0.3.5 imdb_data_load.py vyadav bd_project_data

above command should load data in imdb_movie_data cassandra table.

Step 6) Load the test data. Place test_movie_metadata.csv file on HDFS in bd_project_data folder. 
Also, place imdb_test_data_load.py on unix server.

Create cassandra table using script imdb_movie_test_data.cql

Execute below command to load csv test data into cassandra table imdb_movie_test_data.

spark-submit --master=yarn-client --packages com.databricks:spark-csv_2.11:1.5.0,TargetHolding/pyspark-cassandra:0.3.5 imdb_test_data_load.py vyadav bd_project_data

Above script should populate imdb_movie_test_data cassandra table.

Step 7) Create cassandra table using imdb_test_rating.cql for holding predicted ratings.

Step 8) Execute below command to train the models and load the predicted ratings in imdb_test_ratings cassandra table.

spark-submit --master=yarn-client --packages com.databricks:spark-csv_2.11:1.5.0,TargetHolding/pyspark-cassandra:0.3.5 imdb_movielens_data.py vyadav 2>/dev/null

Output Sample -

vyadav@nml-cloud-220:~$ spark-submit --master=yarn-client --packages com.databricks:spark-csv_2.11:1.5.0,TargetHolding/pyspark-cassandra:0.3.5 imdb_movielens_data.py vyadav 2>/dev/null
Learned Decision tree model:
Classifier trained in 45.018 seconds
Prediction made in 14.284 seconds. Decision Tree Validation accuracy is 0.5555
Learned Random Forest tree model:
Prediction made in 15.647 seconds. Random Forest Validation accuracy is 0.5338
Learned Naive Bayes classification model:
Classifier trained in 8.783 seconds
Prediction made in 14.428 seconds. Naive Bayes Validation accuracy is 0.1898
***********************************************************
+--------------------+------+
|         movie_title|rating|
+--------------------+------+
|          Fist Fight|     3|
|Beauty and the Beast|     3|
| I Am Not Your Negro|     4|
|        The Comedian|     3|
|       Power Rangers|     3|
|  Kong: Skull Island|     3|
|                Kedi|     3|
|       Live by Night|     3|
|             Befikre|     3|
|Underworld: Blood...|     3|
|       The Last Word|     3|
|Guardians of the ...|     3|
|    My Cousin Rachel|     3|
|           Lowriders|     3|
|        Toni Erdmann|     4|
|    T2 Trainspotting|     3|
|King Arthur: Lege...|     3|
|               Rings|     4|
|                Sing|     3|
|       The Boss Baby|     3|
+--------------------+------+
only showing top 20 rows

None
***********************************************************
vyadav@nml-cloud-220:~$ spark-submit --master=yarn-client --packages com.databricks:spark-csv_2.11:1.5.0,TargetHolding/pyspark-cassandra:0.3.5 imdb_movielens_data.py vyadav 2>/dev/null
Learned Decision tree model:
Classifier trained in 41.986 seconds
Prediction made in 5.984 seconds. Decision Tree Validation accuracy is 0.5571
Learned Random Forest tree model:
Prediction made in 6.756 seconds. Random Forest Validation accuracy is 0.558
Learned Naive Bayes classification model:
Classifier trained in 21.254 seconds
Prediction made in 4.613 seconds. Naive Bayes Validation accuracy is 0.1903
***********************************************************
+--------------------+------+
|         movie_title|rating|
+--------------------+------+
|          Fist Fight|     3|
|Beauty and the Beast|     3|
| I Am Not Your Negro|     3|
|        The Comedian|     3|
|       Power Rangers|     2|
|  Kong: Skull Island|     3|
|                Kedi|     4|
|       Live by Night|     3|
|             Befikre|     3|
|Underworld: Blood...|     3|
|       The Last Word|     3|
|Guardians of the ...|     3|
|    My Cousin Rachel|     3|
|           Lowriders|     2|
|        Toni Erdmann|     3|
|    T2 Trainspotting|     3|
|King Arthur: Lege...|     3|
|               Rings|     2|
|                Sing|     3|
|       The Boss Baby|     3|
+--------------------+------+
only showing top 20 rows

None
***********************************************************



Step 9) Check the rating of predicted rating for upcoming movie in cassandra table imdb_test_ratings.

Note - Unix command to convert non-ascii charater to utf-8 in case any of the csv file throws non-ascii character error.
iconv -f UTF8 -t US-ASCII//TRANSLIT movie_metadata.csv > movie_metadata_tmp.csv
mv movie_metadata_tmp.csv movie_metadata.csv
