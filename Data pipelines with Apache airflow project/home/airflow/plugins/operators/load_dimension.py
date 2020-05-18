from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_into_stmt = """
        INSERT INTO {table} 
        {select_query}
    """
    


    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id,
                 table,
                 select_query,   
                 truncate_table=True,                       
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table=table
        self.truncate_table=truncate_table
        self.select_query=select_query
        
        

    def execute(self, context):
        self.log.info('connect to redshift...')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_table:
            self.log.info(f'Truncating Table {self.table}')
            redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Inserting dimension table data...")
        redshift.run(LoadDimensionOperator.insert_into_stmt.format(
            table=self.table,
            select_query=self.select_query
        ))
        
        
        

