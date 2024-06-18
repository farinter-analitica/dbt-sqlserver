import os
from dotenv import load_dotenv
import subprocess

# Define the relative path to the .env file
env_path = os.path.join(os.path.dirname(__file__), '.env')

# Load environment variables from .env file if it exists
if os.path.exists(env_path):
    load_dotenv(env_path)

    # Define paths for --project-dir and --profiles-dir
    project_dir = os.path.join(os.path.dirname(__file__), '')
    profiles_dir = os.path.join(os.path.dirname(__file__), '')

    # Run dbt run command with proper directory paths
    subprocess.run([
        'dbt', 'run',
        '--project-dir', project_dir,
        '--profiles-dir', profiles_dir,
        '--target', 'dev'
    ])
else:
    print('.env file not found in the script directory. Please create it.')
