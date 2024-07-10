from dagster import ConfigurableResource import psycopg2  
# Define the resource class with configuration schema using type annotations 
class PostgresResource(ConfigurableResource):     
    host: str     
    port: int     
    dbname: str     
    user: str     
    password: str      
    def connect(self):         
        return psycopg2.connect(             host=self.host,             port=self.port,             dbname=self.dbname,             user=self.user,             password=self.password         )  
    # Example usage in an asset @asset(required_resource_keys={"postgres"}) 
    