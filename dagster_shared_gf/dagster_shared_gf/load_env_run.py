import os
from pathlib import Path
from dotenv import load_dotenv

def load_env_vars(joinpath_str: list[str] = ["..",".."],file_name:str = ".env") -> None:
    """
    Loads env vars from .env file
    """	
    # Define the relative path to the .env file
    base_os_path = os.path.dirname(__file__)
    workspace_dir = Path(base_os_path).joinpath(*joinpath_str).resolve()
    env_path = os.path.join(workspace_dir, file_name)

    if os.path.exists(env_path):
        print(f"Loading .env file from {env_path}.")
        load_dotenv(env_path)
    else:
        print(f".env file not found in the script directory {env_path}.")


if __name__ == "__main__":
    load_env_vars()
#load_env_vars()
