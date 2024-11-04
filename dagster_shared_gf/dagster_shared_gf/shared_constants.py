# Refined regex pattern to handle all specified cases with length validation
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
) # Missing start with dot, ending with dot and double dot validations
CONSECUTIVE_DOTS_REGEX_PATTERN = r"\.\."
ENDING_DOT_REGEX_PATTERN = r"\.$"
STARTING_DOT_REGEX_PATTERN = r"^\."
LOCAL_PART_ENDS_WITH_DOT_PATTERN = r".*\.@.*"
EMAIL_REGEX_INVALID_DOTS_PATTERN = r".*\.\..*|.*\.$|^\..*|.*\.@.*"