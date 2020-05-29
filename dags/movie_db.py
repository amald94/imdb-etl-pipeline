from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (GetMovieDetails,StageToRedshiftOperator,
                               DataQualityOperator,LoadMovieTable,LoadDirectorTable)
from helpers import SqlQueries
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

s3_bucket = 'moviedb-etl'

default_args = {
    'owner': 'Amal Das',
    'start_date': days_ago(2),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag_name = 'moviedb_etl_v4'

dag = DAG(dag_name,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs = 1       
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

get_movie_details = GetMovieDetails(
    task_id="prepare_data",
    dag=dag,
    dataset="dataset/movie_metadata.csv"
)

stage_movies_to_redshift = StageToRedshiftOperator(
 task_id='Stage_movies',
    table_name="staging_movies",
    s3_bucket = s3_bucket,
    s3_key = "movies",
    redshift_conn_id = "redshift",
    aws_credential_id="aws_credentials",
    dag=dag,
    provide_context=True
)

stage_director_to_redshift = StageToRedshiftOperator(
 task_id='Stage_director',
    table_name="staging_director",
    s3_bucket = s3_bucket,
    s3_key = "director",
    redshift_conn_id = "redshift",
    aws_credential_id="aws_credentials",
    dag=dag,
    provide_context=True
)

load_movie_table = LoadMovieTable(
    task_id='Load_movie_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    sql_query = SqlQueries.movie_table_insert
)

load_top_ten_movies = LoadMovieTable(
    task_id='Load_top_ten_movies',
    dag=dag,
    redshift_conn_id = 'redshift',
    sql_query = SqlQueries.top_ten_movies
)

load_best_movie_of_decade = LoadMovieTable(
    task_id='Load_rated_movies_decade',
    dag=dag,
    redshift_conn_id = 'redshift',
    sql_query = SqlQueries.top_movies_decade
)

load_director_tables = LoadDirectorTable(
    task_id='Load_director_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    sql_query = SqlQueries.director_table_insert
)

load_top_gross_movies = LoadDirectorTable(
    task_id='top_gross_movies_by_directors',
    dag=dag,
    redshift_conn_id = 'redshift',
    sql_query = SqlQueries.top_gross_films_by_category
)


table_to_check = ["director_table","top_gross_films", 
                  "movie_list","top_movies_decade", "top_ten_movies"]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    tables = table_to_check
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> get_movie_details >> [stage_movies_to_redshift, stage_director_to_redshift] 

stage_movies_to_redshift >> [load_movie_table, load_top_ten_movies, load_best_movie_of_decade] >> run_quality_checks

stage_director_to_redshift >> [load_director_tables,load_top_gross_movies] >> run_quality_checks

run_quality_checks >> end_operator
