from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from helpers import create_tables



class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql = """
        COPY {schema}.{table}
        FROM 's3://{s3_bucket}/{s3_key}/{s3_prefix}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {copy_options}
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 schema="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 options="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.options = options

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Creating {self.table} table")
        if self.table == 'immigration':
            redshift.run(create_tables.CREATE_IMMIGRATION_TABLE_SQL)
        elif self.table == 'country':
            redshift.run(create_tables.CREATE_COUNTRY_TABLE_SQL)
        elif self.table == 'state':
            redshift.run(create_tables.CREATE_STATE_TABLE_SQL)
        elif self.table == 'date':
            redshift.run(create_tables.CREATE_DATE_TABLE_SQL)
        
        self.log.info("Copying data from S3 to Redshift")
        copy_options = '\n\t\t\t'.join(self.options)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.schema,
            self.table,
            self.s3_key,
            self.s3_prefix,
            credentials.access_key,
            credentials.secret_key,
            copy_options
        )
        redshift.run(formatted_sql)





