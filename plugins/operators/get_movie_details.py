from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
import urllib
from omdb import OMDBClient
from airflow.models import Variable
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import isnan, when, count, col

class GetMovieDetails(BaseOperator):

    @apply_defaults
    def __init__(self,
                 s3_bucket="",
                 s3_key = "",
                 *args, **kwargs):

        super(GetMovieDetails, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        self.log.info('Getting the movie detials')
        spark = SparkSession.builder.appName('moviedb-etl')\
                .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
                .getOrCreate()

        #path
        s3_path = "s3://{}".format(self.s3_bucket)
        s3_path = s3_path + '/' + self.s3_key
        #read the dataset
        df = spark.read.csv(s3_path,
                   header=True)
        
        ## Prepare movie and director tables
        # extract columns to create movie table
        movie_fields = ["movie_title as title", "imdb_score as rating", 
                        "title_year as year", "duration", "director_name as director", 
                        "gross", "genres", "num_user_for_reviews as votes", 
                        "content_rating as content", "budget"]
        movie_table = df.selectExpr(movie_fields).dropDuplicates()
        movie_table.show(5)

        # extract columns to create director table
        director_fields = ["director_name", "gross", "genres", "movie_title",
                         "content_rating", "budget", "imdb_score as rating"]
        director_table = df.selectExpr(director_fields).dropDuplicates()
        director_table.show(5)

        # null value check
        director_table.select([count(when(isnan(c), c)).alias(c) for c in director_table.columns]).show()
        movie_table.select([count(when(isnan(c), c)).alias(c) for c in movie_table.columns]).show()

        # write the generated dataframe back to s3
        s3_processed = "s3://{}".format(self.s3_bucket)
        s3_processed = s3_processed + '/' + 'processed'
        s3_movies = s3_processed + '/' + 'movies.csv'
        s3_direcor = s3_processed + '/' + 'director.csv'
        movie_table.write.csv(s3_movies,mode="overwrite")
        director_table.write.csv(s3_direcor,mode="overwrite")




