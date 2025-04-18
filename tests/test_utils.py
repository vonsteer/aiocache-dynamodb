from aiocache_dynamodb import utils


def test_is_numerical_invalid_input() -> None:
    """Test is_numerical with invalid input."""
    assert utils.is_numerical("abc") is False
    assert utils.is_numerical("{ hello}") is False
    assert utils.is_numerical("123abc") is False


def test_is_numerical_valid_input() -> None:
    """Test is_numerical with valid input."""
    assert utils.is_numerical("123") is True
    assert utils.is_numerical("123.45") is True
    assert utils.is_numerical("0") is True
    assert utils.is_numerical("-123") is True
    assert utils.is_numerical("-123.45") is True
