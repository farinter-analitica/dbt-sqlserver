# Refined regex pattern to handle all specified cases with length validation
from datetime import timedelta
from enum import Enum

from dagster import DefaultSensorStatus

from dagster_shared_gf.shared_functions import get_for_current_env

EMAIL_REGEX_PATTERN = (
    r"^(?!.{255})"  # negative lookahead to check total length does not exceed 254 characters
    r"(?!.*\.\.)(?!.*\.$)(?!^\.)"  # negative lookaheads for consecutive dots, ending with a dot, and starting with a dot
    r"[a-z0-9._%+-]{1,64}(?<!\.)@"  # local part, ensuring it does not end with a dot and has a maximum length of 64 characters
    r"(?:"
    r"\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\]|"  # IP address
    r"(?!.{256})"  # negative lookahead to check domain part length does not exceed 255 characters
    r"(?:[a-z0-9-]{1,63}\.){0,}[a-z0-9-]{1,63}\.[a-z]{2,}"  # domain name with optional subdomains
    r")$"
)
EMAIL_REGEX_PATTERN_RUST_CRATES = (
    r"[a-z0-9._%+-]{1,64}@"  # local part, ensuring it does not end with a dot and has a maximum length of 64 characters
    r"(?:"
    r"\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\]|"  # IP address
    r"(?:[a-z0-9-]{1,63}\.){0,}[a-z0-9-]{1,63}\.[a-z]{2,}"  # domain name with optional subdomains
    r")$"
)  # Missing start with dot, ending with dot and double dot validations
CONSECUTIVE_DOTS_REGEX_PATTERN = r"\.\."
ENDING_DOT_REGEX_PATTERN = r"\.$"
STARTING_DOT_REGEX_PATTERN = r"^\."
LOCAL_PART_ENDS_WITH_DOT_PATTERN = r".*\.@.*"
EMAIL_REGEX_INVALID_DOTS_PATTERN = r".*\.\..*|.*\.$|^\..*|.*\.@.*"

only_dev_running_default_sensor_status: DefaultSensorStatus = get_for_current_env(
    {
        "local": DefaultSensorStatus.STOPPED,
        "dev": DefaultSensorStatus.RUNNING,
        "prd": DefaultSensorStatus.STOPPED,
    }
)
only_prd_running_default_sensor_status: DefaultSensorStatus = get_for_current_env(
    {
        "local": DefaultSensorStatus.STOPPED,
        "dev": DefaultSensorStatus.STOPPED,
        "prd": DefaultSensorStatus.RUNNING,
    }
)
hourly_freshness_seconds_per_environ: int = get_for_current_env(
    {
        "dev": 60 * 60 * 6,  # 6 horas
        "prd": 60 * 60 * 1,
    }
)
hourly_freshness_lbound_per_environ: timedelta = get_for_current_env(
    {
        "dev": timedelta(hours=30),  # 30 horas
        "prd": timedelta(hours=13),
    }
)
running_default_sensor_status: DefaultSensorStatus = get_for_current_env(
    {
        "local": DefaultSensorStatus.STOPPED,
        "dev": DefaultSensorStatus.RUNNING,
        "prd": DefaultSensorStatus.RUNNING,
    }
)
stopped_default_sensor_status: DefaultSensorStatus = get_for_current_env(
    {
        "local": DefaultSensorStatus.STOPPED,
        "dev": DefaultSensorStatus.STOPPED,
        "prd": DefaultSensorStatus.STOPPED,
    }
)


class RowTerminator(Enum):
    CRLF = "\\r\\n"  # Windows-style
    LF = "\\n"  # Unix-style
    CR = "\\r"  # Old Mac-style

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return f"RowTerminator.{self.name}"
