import os
from dotenv import load_dotenv
from pathlib import Path
import subprocess


# os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"]="python"
def parse():
    autoload(command="parse")


def deps():
    autoload(command="deps")


def run():
    autoload(command="run")


def compile():
    autoload(command="compile")


def build():
    autoload(command="build")


def debug():
    autoload(command="debug")


def autoload(command: str = "parse", select: str = ""):
    print("Loading .env file if it exists.")
    load_dotenv()

    # Define paths for --project-dir and --profiles-dir

    base_os_path = os.environ.get("DAGSTER_HOME")
    env_str = os.environ.get("DAGSTER_INSTANCE_CURRENT_ENV", "dev")
    if not base_os_path or not env_str:
        raise ValueError(
            "DAGSTER_HOME or DAGSTER_INSTANCE_CURRENT_ENV environment variable is not set."
        )

    project_dir = Path(base_os_path) / "dbt_dwh_farinter"
    profiles_dir = Path(base_os_path) / "dbt_dwh_farinter"
    if command == "":
        command = "parse"

    # Normalizar target
    env_str = "prd" if env_str == "prd" else "dev"

    # Run dbt run command with proper directory paths
    run_options = [
        command,
        "--project-dir",
        project_dir,
        "--profiles-dir",
        profiles_dir,
        "--target",
        env_str,
    ]
    if select != "":
        run_options.append("--select")
        run_options.append(select)

    # print (run_options)
    subprocess.run(["dbt", *run_options])

    print(f"dbt completed successfully: {command} {select}")


def ask():
    command: str = ""
    select: str = ""
    commands: list = [
        "parse",
        "run",
        "compile",
        "build",
        "debug",
        "clean",
        "snapshot",
        "deps",
    ]
    command_map = {
        "p": "parse",
        "r": "run",
        "c": "compile",
        "b": "build",
        "d": "debug",
        "s": "snapshot",
    }

    print(
        "Choose one of the following commands: [p]arse, [r]un, [c]ompile, [b]uild, [d]ebug, clean, [s]napshot, deps. Default is parse."
    )
    command = input().strip().lower()

    # Fix when one letter provided, check first letter and select the right one
    if len(command) == 1:
        if command in command_map:
            command = command_map[command]

    # Verify the command is valid
    if command not in commands:
        command = "parse"  # Default to parse if the command is not in the list

    if command in ["run", "build", "snapshot"]:
        print("Add a model to select. Default is all.")
        select = input()

    autoload(command, select)


if __name__ == "__main__":
    ask()
