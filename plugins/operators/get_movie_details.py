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
                 dataset="",
                 *args, **kwargs):

        super(GetMovieDetails, self).__init__(*args, **kwargs)
        self.dataset = dataset

    def execute(self, context):
        self.log.info('Getting the movie detials')
        spark = SparkSession.builder.appName('Basics')\
                .getOrCreate()
            
        #read the dataset
        df = spark.read.csv(self.dataset,
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

        # write the generated dataframe back to s3 in parquet format
        movie_table.write.csv("movie.csv",mode="overwrite")
        director_table.write.csv("director.csv",mode="overwrite")




