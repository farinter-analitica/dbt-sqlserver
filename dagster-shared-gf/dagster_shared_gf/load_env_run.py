from dagster_shared_gf.config import load_env_file


def load_env_vars(
    joinpath_str: list[str] | None = None, file_name: str = ".env"
) -> None:
    """Compatibility wrapper that delegates to central config loader."""
    load_env_file(joinpath_str=joinpath_str, file_name=file_name)


if __name__ == "__main__":
    load_env_vars()
