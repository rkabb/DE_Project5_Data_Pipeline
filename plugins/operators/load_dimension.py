from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    table_truncate = """ TRUNCATE table   {} """

    Insert_stmt = """    
    INSERT INTO {}
    {}
    """

    @apply_defaults
    def __init__(self,
                 table_name="",
                 redshift_conn_id="",
                 sql_stmt="",
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.redshift_conn_id = redshift_conn_id
        self.sql_stmt = sql_stmt
        self.mode = kwargs["params"]["mode"]

    def execute(self, context):
        self.log.info('Starting LoadDimensionOperator ')
        redshift = PostgresHook(self.redshift_conn_id)

        # check what is load mode. If append, do not delete
        if self.mode == 'reload':
            self.log.info('Truncate table')
            redshift.run(LoadDimensionOperator.table_truncate.format(self.table_name))

        self.log.info('Insert Data to table  ')
        redshift.run(LoadDimensionOperator.Insert_stmt.format(self.table_name, self.sql_stmt))
