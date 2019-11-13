from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql = ("""copy {}
                   from '{}'
                   access_key_id '{}'
                   secret_access_key '{}'
                   {}
    """)
    # for staging_songs last parameter should be "json 'auto'"
    # for staging_events last parameter should be "format as json json_path"
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_path="",
                 s3_json_format="json 'auto'",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.s3_path=s3_path
        self.s3_json_format=s3_json_format

    def execute(self, context):
      
        aws_hook=AwsHook(self.aws_credentials_id)
        credentials=aws_hook.get_credentials()
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('self.redshift_conn_id: {}'.format(self.redshift_conn_id))
        
        self.log.info("Clearing data from destination Redshift {} table".format(self.table) )
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift {} table".format(self.table) )
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            self.s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.s3_json_format
        )
        #self.log.info(formatted_sql)
        redshift.run(formatted_sql)




