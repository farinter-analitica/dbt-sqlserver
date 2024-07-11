import pytest
import importlib

def test_mock_with_side_effect(mocker):
    m = mocker.Mock()
    m.side_effect = ['Pytest', 'with', 'Eric']

    assert m() == 'Pytest'
    assert m() == 'with'
    assert m() == 'Eric'

# Test using the mocker fixture for mocking
def test_mock_obj_with_mocker(mocker):
    # Create a mock object with a single return value using mocker
    mock_obj = mocker.Mock(return_value=42)
    
    # Now, you can use mock_obj just like a function or method
    result = mock_obj()
    
    # Assert that the result is equal to the mock return value
    assert result == 42


from dagster_shared_gf_tests.examples.example_interest_calculator import calculate_interest, define_interest_rate

def test_return_different_interest_rates_based_on_account_type(mocker):
    mocker.patch(
        "dagster_shared_gf_tests.examples.example_interest_calculator.define_interest_rate",
        side_effect=lambda account_type: {
            "saving": 0.02,
            "current": 0.01,
        }.get(account_type, 0.005),
    )

    result_saving = calculate_interest("saving", 1000)
    result_current = calculate_interest("current", 1000)
    result_default = calculate_interest("default", 1000)

    assert result_saving == 20.0
    assert result_current == 10.0
    assert result_default == 5.0

import pytest

def test_side_effect_exhaustion(mocker):
    # Create a MagicMock with side_effect as an iterable
    mock_iterable = mocker.MagicMock()
    mock_iterable.side_effect = [1, 2, 3]

    # Consume all elements from the iterable
    assert mock_iterable() == 1
    assert mock_iterable() == 2
    assert mock_iterable() == 3

    # Further calls will raise StopIteration
    with pytest.raises(StopIteration):
        mock_iterable()

