from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'Rekha Kabbur',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False,
    'email': ['rvkabbur@yahoo.com'],
    'schedule_interval': '@hourly'
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow'
          )

begin_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table_name='staging_songs',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    copy_format="format as json 'auto'"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table_name='staging_events',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    copy_format="format as json 's3://udacity-dend/log_json_path.json'"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table_name='songplays',
    redshift_conn_id="redshift",
    sql_stmt=SqlQueries.songplay_table_insert,
    params={
        'mode': 'reload',
    }
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table_name='users',
    redshift_conn_id="redshift",
    sql_stmt=SqlQueries.user_table_insert,
    provide_context=True,
    params={
        'mode': 'reload',
    }
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table_name='songs',
    redshift_conn_id="redshift",
    sql_stmt=SqlQueries.song_table_insert,
    provide_context=True,
    params={
        'mode': 'reload',
    }
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table_name='artists',
    redshift_conn_id="redshift",
    sql_stmt=SqlQueries.artist_table_insert,
    provide_context=True,
    params={
        'mode': 'reload',
    }
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table_name='time',
    redshift_conn_id="redshift",
    sql_stmt=SqlQueries.time_table_insert,
    provide_context=True,
    params={
        'mode': 'reload',
    }
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    provide_context=True,
    params={
        'test1': 'rowcount',
    }
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

begin_operator >> stage_songs_to_redshift
begin_operator >> stage_events_to_redshift
stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
