import pytest
import re
from dagster_shared_gf.shared_constants import (
    EMAIL_REGEX_PATTERN,
    EMAIL_REGEX_PATTERN_RUST_CRATES,
    EMAIL_REGEX_INVALID_DOTS_PATTERN
)

email_base_test_cases = [
    ("test@ejemplo.com", True),
    ("test.correoe@ejemplo.com", True),
    ("test.correoe+alias@ejemplo.com", True),
    ("test.correoe+alias@ejemplo.co.uk", True),
    ("test@[123.100.2.105]", True),
    ("test.correoe.@ejemplo.com", False),
    ("test..correoe@ejemplo.com", False),
    ("test.correoe@ejemplo..com", False),
    ("test.@ejemplo.com", False),
    ("@ejemplo.com", False),
    ("test@.ejemplo.com", False),
    ("test@example", False),  # missing top-level domain
    ("test@example.", False),  # missing top-level domain
    ("test@example.com.au", True),  # valid country-code top-level domain
    ("test@example.co", True),  # valid country-code top-level domain
    ("test@example.io", True),  # valid generic top-level domain
    ("test@example.museum", True),  # valid sponsored top-level domain
    ("test@example.travel", True),  # valid sponsored top-level domain
    (".test@example", False),  # invalid starting dot
    ("test@.example.com", False),  # invalid domain name
    ("test@example.com@example.com", False),  # invalid email address
    ("test@example.com@example", False),  # invalid email address
    # Invalid because local part exceeds 64 characters
    ("a" * 65 + "@example.com", False),
    # Invalid because a domain label exceeds 63 characters
    ("test@" + "a" * 64 + ".com", False),
    # Invalid because domain part exceeds 255 characters
    ("test@" + ("a" * 63 + ".") * 4 + "com", False),
    # Invalid because total length exceeds 254 characters
    ("a" * 64 + "@" + ("b" * 63 + ".") * 3 + "com", False),
]


def test_email_regex():
    for email, expected in email_base_test_cases:
        result = bool(re.match(EMAIL_REGEX_PATTERN, email, re.IGNORECASE))
        assert (
            result == expected
        ), f"Email: {email}, Expected: {expected}, Result: {result}"


def test_email_regex_rust_crates():
    for email, expected in email_base_test_cases:
        partial_result = bool(
            re.match(EMAIL_REGEX_PATTERN_RUST_CRATES, email, re.IGNORECASE)
        )
        if (
            email.startswith(".")
            or email.endswith(".")
            or email.find(r"..") != -1
            or len(email) > 254
            or email.find(r".@") != -1
        ):
            result = False
        else:
            result = partial_result
        assert (
            result == expected
        ), f"Email: {email}, Expected: {expected}, Result: {result}"

def test_email_regex_rust_crates_double_regex():
    for email, expected in email_base_test_cases:
        partial_result = bool(
            re.match(EMAIL_REGEX_PATTERN_RUST_CRATES, email, re.IGNORECASE)
        )
        if bool(
            re.match(EMAIL_REGEX_INVALID_DOTS_PATTERN, email, re.IGNORECASE)
        ) or len(email) > 254:
            result = False
        else:
            result = partial_result
        assert (
            result == expected
        ), f"Email: {email}, Expected: {expected}, Result: {result}"


@pytest.mark.skip(reason="Not implemented")
def test_email_regex_ind():
    test_cases = [
        ("test@example.xn-p1ai", True),  # valid internationalized domain name
    ]
    for email, expected in test_cases:
        result = bool(re.match(EMAIL_REGEX_PATTERN, email, re.IGNORECASE))
        assert (
            result == expected
        ), f"Email: {email}, Expected: {expected}, Result: {result}"
