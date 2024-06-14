from dagster import ConfigurableResource, InitResourceContext
import psycopg2

class PostgresResource(ConfigurableResource):
    def __init__(self, context: InitResourceContext):
        self.host = context.resource_config['host']
        self.user = context.resource_config['user']
        self.password = context.resource_config['password']
        self.db_name = context.resource_config['db_name']

    def connect(self):
        return psycopg2.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            dbname=self.db_name
        )

#
# Example usage in an asset
#@asset(required_resource_keys={'postgres'})
# def my_asset(context):
#     postgres_resource = context.resources.postgres
#     with postgres_resource.connect() as conn:
#         # Perform database operations using the connection
#         ...