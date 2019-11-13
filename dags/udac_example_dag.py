from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# We're going to create two connections 
# 1. Open Admin->Connections
# 2. Click "Create"
# 3. Set "Conn Id" to "aws_credentials", "Conn Type" to "Amazon Web Services"
# 4. Set "Login" to your aws_access_key_id and "Password" to your aws_secret_key
# 5. Click save
# 6. Click "Create"
# 7. Set "Conn Id" to "redshift", "Conn Type" to "Postgress"
# 8. Set "Host", "Schema", "Username" and "Password" to your redhsift cluster
# 9. Click save

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    #'schedule_interval': '@hourly',
    'schedule_interval': None,
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
    'max_active_runs': 1,
    'redshift_conn_id': 'redshift',
    'aws_credentials_id': 'aws_credentials',
    'append_or_replace': 'replace'
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          #schedule_interval='0 * * * *'
          schedule_interval=None
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    s3_path="s3://udacity-dend/log_data",
    s3_json_format="format as json 's3://udacity-redshift-dw/log_json_path.json'",
    table="staging_events"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    s3_path="s3://udacity-dend/song_data",
    table="staging_songs"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    sql_query=SqlQueries.songplay_table_insert,
    table="songplays"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    sql_query=SqlQueries.user_table_insert,
    table="users"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    sql_query=SqlQueries.song_table_insert,
    table="songs"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    sql_query=SqlQueries.artist_table_insert,
    table="artists"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    sql_query=SqlQueries.time_table_insert,
    table="time"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    test_query="select count(1) from {}"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#start_operator >> run_quality_checks >> end_operator


start_operator                   \
>> [ stage_events_to_redshift,   \
     stage_songs_to_redshift     \
   ]                             \
>> load_songplays_table          \
>> [ load_user_dimension_table,  \
     load_song_dimension_table,  \
     load_artist_dimension_table,\
     load_time_dimension_table,  \
   ]                             \
>> run_quality_checks            \
>> end_operator

