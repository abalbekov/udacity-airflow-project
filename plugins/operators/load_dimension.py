from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 sql_query="",
                 table="",
                 append_or_replace="replace",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.sql_query=sql_query
        self.table=table
        self.append_or_replace=append_or_replace

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append_or_replace.lower()=="replace" :
            self.log.info(f"Deleting from {self.table}")
            redshift.run("DELETE FROM {}".format(self.table))
            
        self.log.info(f"Loading into {self.table}")
        redshift.run(self.sql_query)

