def is_numerical(value: str) -> bool:
    """Check if the string is a digit or a float."""
    try:
        if value.isdigit():
            return True
        float(value)
        return True
    except (ValueError, TypeError):
        return False
