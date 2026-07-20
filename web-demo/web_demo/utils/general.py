def bytes_to_human_readable(bytes_value: float) -> str:
    """
    Convert bytes to human readable format with up to 1 decimal point.

    Args:
        bytes_value: The number of bytes to convert

    Returns:
        A string representation with appropriate unit (B, KB, MB, GB, TB, PB)

    Examples:
        >>> bytes_to_human_readable(1024)
        '1.0 KB'
        >>> bytes_to_human_readable(1572864)
        '1.5 MB'
    """
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    unit_index = 0
    value = float(bytes_value)

    while value >= 1024.0 and unit_index < len(units) - 1:
        value /= 1024.0
        unit_index += 1

    if unit_index == 0:
        return f"{int(value)} {units[unit_index]}"

    return f"{value:.1f} {units[unit_index]}"

def get_source_code(func):
    import inspect
    import textwrap
    lines = inspect.getsource(func)
    lines = textwrap.dedent(lines)
    return lines
