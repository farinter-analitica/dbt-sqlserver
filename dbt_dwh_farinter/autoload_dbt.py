import os
from pathlib import Path
from dotenv import load_dotenv
import subprocess

# Define the relative path to the .env file
base_os_path = os.path.dirname(__file__)
dbt_project_dir = Path(base_os_path).joinpath("..").resolve()
env_path = os.path.join(dbt_project_dir, '.env')
#os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"]="python"
def parse():
    autoload(command="parse")

def run():
    autoload(command="run")

def compile():
    autoload(command="compile")

def build():
    autoload(command="build")

def debug():
    autoload(command="debug")

def autoload(command: str = "parse", select: str = ""):
    # Load environment variables from .env file if it exists
    if os.path.exists(env_path):
        print(f"Loading .env file from {env_path}.")
        load_dotenv(env_path)

        # Define paths for --project-dir and --profiles-dir
        project_dir = os.path.join(base_os_path, '')
        profiles_dir = os.path.join(base_os_path, '')
        if command == "":
            command = "parse"

        # Run dbt run command with proper directory paths
        run_options = [command, '--project-dir', project_dir, '--profiles-dir', profiles_dir, '--target', 'dev']
        if select != "":
            run_options.append("--select")
            run_options.append(select)

        #print (run_options)
        subprocess.run([
            'dbt',
            *run_options
        ])

        print(f"dbt completed successfully: {command} {select}")
    else:
        print('.env file not found in the script directory. Please create it.')

def ask():
    command: str = ""
    select: str = ""
    commands: list = ["parse", "run", "compile", "build", "debug", "clean", "snapshot"]
    command_map = {
        'p': "parse",
        'r': "run",
        'c': "compile",
        'b': "build",
        'd': "debug",
        's': "snapshot"
    }

    print("Choose one of the following commands: [p]arse, [r]un, [c]ompile, [b]uild, [d]ebug, clean, [s]napshot. Default is parse.")
    command = input().strip().lower()

    # Fix when one letter provided, check first letter and select the right one
    if len(command) == 1:
        if command in command_map:
            command = command_map[command]

    # Verify the command is valid
    if command not in commands:
        command = "parse"  # Default to parse if the command is not in the list

    if command in [ "run", "build", "snapshot"]:
        print("Add a model to select. Default is all.")
        select = input()

    autoload(command, select)
    
if __name__ == "__main__":
    ask()