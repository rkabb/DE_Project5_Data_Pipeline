from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = "redshift"
        self.test1 = kwargs["params"]["test1"]

    def execute(self, context):
        self.log.info('Running DataQualityOperator')
        redshift = PostgresHook(self.redshift_conn_id)

        if self.test1 == 'rowcount':
            songs_records = redshift.get_records("SELECT COUNT(*) FROM SONGS")
            users_records = redshift.get_records("SELECT COUNT(*) FROM USERS")
            time_records = redshift.get_records("SELECT COUNT(*) FROM time")
            artists_records = redshift.get_records("SELECT COUNT(*) FROM ARTISTS")
            songplays_records = redshift.get_records("SELECT COUNT(*) FROM SONGPLAYS")

            # check if any of the count is zero
            if len(songs_records) < 1 or len(songs_records[0]) < 1:
                raise ValueError(f"Data quality check failed. SONGS returned no results")
            song_rec_cnt = songs_records[0][0]
            if song_rec_cnt < 1:
                raise ValueError(f"Data quality check failed. SONGS contained 0 rows")
            logging.info(f"Data quality on table SONGS check passed with {songs_records[0][0]} records")

            if len(users_records) < 1 or len(users_records[0]) < 1:
                raise ValueError(f"Data quality check failed. USERS returned no results")
            usr_rec_cnt = users_records[0][0]
            if usr_rec_cnt < 1:
                raise ValueError(f"Data quality check failed. USERS contained 0 rows")
            logging.info(f"Data quality on table USERS check passed with {users_records[0][0]} records")

            if len(time_records) < 1 or len(time_records[0]) < 1:
                raise ValueError(f"Data quality check failed. TIME returned no results")
            time_rec_cnt = time_records[0][0]
            if time_rec_cnt < 1:
                raise ValueError(f"Data quality check failed. TIME contained 0 rows")
            logging.info(f"Data quality on table TIME check passed with {time_records[0][0]} records")

            if len(artists_records) < 1 or len(artists_records[0]) < 1:
                raise ValueError(f"Data quality check failed. ARTISTS returned no results")
            artist_rec_cnt = artists_records[0][0]
            if artist_rec_cnt < 1:
                raise ValueError(f"Data quality check failed. ARTISTS contained 0 rows")
            logging.info(f"Data quality on table ARTISTS check passed with {artists_records[0][0]} records")

            if len(songplays_records) < 1 or len(songplays_records[0]) < 1:
                raise ValueError(f"Data quality check failed. SONGPLAYS returned no results")
            plays_rec_cnt = songplays_records[0][0]
            if plays_rec_cnt < 1:
                raise ValueError(f"Data quality check failed. SONGSPLAYS contained 0 rows")
            logging.info(f"Data quality on table SONGSPLAYS check passed with {songplays_records[0][0]} records")





