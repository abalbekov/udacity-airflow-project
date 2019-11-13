from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    #template_fields = ("test_query",)
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 test_query="select count(1) from {}",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.test_query=test_query

    def execute(self, context):
        redshift_hook=PostgresHook(self.redshift_conn_id)
        
        for table in ["time", "users", "artists", "songs", "songplays"] :
            formatted_test_query=self.test_query.format(table)
            self.log.info(f"Running data quality SQL: {formatted_test_query}")
            records=redshift_hook.get_records(self.test_query.format(table))
            #records=redshift_hook.get_records(f"select count(1) from {table}")
            if records is None or len(records[0])<1 or records[0][0]<1 :
                self.log.error(f"No records found in destination table {table}")
                raise ValueError(f"No records found in destination table {table}")
            
            self.log.info(f"Data quality check passed on table {table} with { records[0][0] } records")
            