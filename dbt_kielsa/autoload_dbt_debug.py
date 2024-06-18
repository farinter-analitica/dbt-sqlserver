import os
from dotenv import load_dotenv
import subprocess

# Load environment variables from .env file
load_dotenv('.env')

# Run dbt debug command
subprocess.run(['dbt', 'debug'])