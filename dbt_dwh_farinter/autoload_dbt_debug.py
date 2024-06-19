import os
from pathlib import Path
from dotenv import load_dotenv
import subprocess

# Define the relative path to the .env file
base_os_path = os.path.dirname(__file__)
dbt_project_dir = Path(base_os_path).joinpath("..").resolve()
env_path = os.path.join(dbt_project_dir, '.env')

# Load environment variables from .env file if it exists
if os.path.exists(env_path):
    print(f"Loading .env file from {env_path}.")
    load_dotenv(env_path)

    # Define paths for --project-dir and --profiles-dir
    project_dir = os.path.join(base_os_path, '')
    profiles_dir = os.path.join(base_os_path, '')

    # Run dbt run command with proper directory paths
    subprocess.run([
        'dbt', 'run',
        '--project-dir', project_dir,
        '--profiles-dir', profiles_dir,
        '--target', 'dev'
    ])
else:
    print(f".env file not found in the script directory {env_path}.  Please create it.")
