from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries


class LoadFactOperator(BaseOperator):
    table_truncate = """ TRUNCATE table {} """

    songplay_table_insert = """
        INSERT INTO {} {}        
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table_name="",
                 redshift_conn_id="",
                 sql_stmt="",
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.redshift_conn_id = redshift_conn_id
        self.sql_stmt = sql_stmt
        self.mode = kwargs["params"]["mode"]

    def execute(self, context):
        self.log.info('Starting LoadFactOperator')
        redshift = PostgresHook(self.redshift_conn_id)

        # check what is load mode. If append, do not delete
        if self.mode == 'reload':
            self.log.info('Truncate table')
            redshift.run(LoadFactOperator.table_truncate.format(self.table_name))

        self.log.info('Insert records to Songplay table ')
        redshift.run(LoadFactOperator.songplay_table_insert.format(
            self.table_name,
            self.sql_stmt)
        )

