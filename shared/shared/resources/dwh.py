import os
from dagster import ConfigurableResource, StringSource, EnvVar

# Define the SQL Server resource
class SqlServerResource(ConfigurableResource):
    current_env = os.environ.get('CURRENT_ENV')

    def get_config(self):
        if self.current_env == 'DEV':
            return {
                'server': os.environ.get('DEV_SQL_SERVER'),
                'database': os.environ.get('DEV_SQL_DATABASE'),
                'username': os.environ.get('DEV_SQL_USERNAME'),
                'password': EnvVar('DEV_SQL_PASSWORD'),
            }
        elif self.current_env == 'PRD':
            return {
                'server': os.environ.get('PRD_SQL_SERVER'),
                'database': os.environ.get('PRD_SQL_DATABASE'),
                'username': os.environ.get('PRD_SQL_USERNAME'),
                'password': EnvVar('PRD_SQL_PASSWORD'),
            }
        else:
            raise ValueError(f"Unknown environment: {self.current_env}")
