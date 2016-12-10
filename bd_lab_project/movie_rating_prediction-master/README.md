===================================================================================
# STEP 1:

# Current working directory should be movie_rating_prediction-master
pwd 

# crawler takes about 48-72 hours to crawl entire data
scrapy crawl imdb -o imdb_output.json

# below python file parses output from crawler into a CSV file
python parse_scraped_data.py


