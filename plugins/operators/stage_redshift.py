from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    table_drop = "DROP table if exists {}"
    staging_event_table_create = """
        CREATE TABLE IF NOT EXISTS staging_events
        (
   artist varchar ,
   auth varchar ,
   firstName varchar ,
   gender CHAR ,
   itemInSession SMALLINT,
   lastName varchar,
   length double precision,
   level varchar  ,
   location varchar ,
   method varchar,
   page varchar,
   registration REAL,
   sessionId INTEGER ,
   song varchar,
   status SMALLINT ,
   ts BIGINT ,
   useragent varchar ,
   userid INTEGER 
           )
        """

    staging_songs_table_create = """
        CREATE TABLE IF NOT EXISTS staging_songs
        (
            num_songs int4,
            artist_id varchar(256),
            artist_name varchar(256),
            artist_latitude numeric(18,0),
            artist_longitude numeric(18,0),
            artist_location varchar(256),
            song_id varchar(256),
            title varchar(256),
            duration numeric(18,0),
            "year" int4)
        """

    copy_table = """
        copy {} 
        from '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {}
        region 'us-west-2';
        """

    @apply_defaults
    def __init__(self,
                 table_name="",
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 copy_format = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.copy_format = copy_format

    def execute(self, context):
        self.log.info('Starting StageToRedshiftOperator')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(self.redshift_conn_id)

        self.log.info('Drop stage_event table')
        redshift.run(StageToRedshiftOperator.table_drop.format(self.table_name))

        self.log.info('Create stage_event table')
        if self.table_name == 'staging_events':
            redshift.run(StageToRedshiftOperator.staging_event_table_create)
        else:
            redshift.run(StageToRedshiftOperator.staging_songs_table_create)

        self.log.info("Copy data from S3 to table")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}/".format(self.s3_bucket, rendered_key)
        redshift.run(StageToRedshiftOperator.copy_table.format(
            self.table_name,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.copy_format
        )
        )
