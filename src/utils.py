import re

def filter_nodes_by_label(labels, regex):
    for key, _ in labels.items():   # iter on both keys and values
        if bool(re.match(regex, key)):
            return True
    return False
    
def parse_memory_to_bytes(memory_string):
    """
    Parses a memory string (e.g., "1000Mi", "2Gi", "500Ki", or plain number for bytes)
    into an integer representing bytes.
    - Supports Ki (Kibibytes), Mi (Mebibytes), Gi (Gibibytes).
    - Units are case-insensitive (e.g., "mi", "Mi" are both Mebibytes).
    - Assumes powers of 1024 (Ki = 1024, Mi = 1024*1024, etc.).
    - Supports decimal values (e.g., "1.5Gi").
    """
    if isinstance(memory_string, (int, float)): # If already a number, assume it's bytes
        return int(memory_string)
    if not isinstance(memory_string, str):
        raise TypeError(f"Memory value must be a string or number, got {type(memory_string)}: '{memory_string}'")

    memory_string = memory_string.strip()

    # Check for plain number first (assumed to be bytes)
    if memory_string.replace('.', '', 1).isdigit(): # Allows for a single decimal point
        return int(float(memory_string))

    # Regex to capture value and unit (Ki, Mi, Gi, case-insensitive)
    # Updated regex to allow for decimal values in the number part
    match = re.match(r"(\d+(\.\d+)?)([KkMmGg]i)$", memory_string)
    if not match:
        raise ValueError(
            f"Invalid memory string format: '{memory_string}'. "
            "Expected format like '1024Ki', '100Mi', '2Gi', '1.5Gi', or a plain number for bytes."
        )

    value = float(match.group(1)) # Use float to handle decimal values
    unit = match.group(3).lower() # Converts unit to lowercase 'ki', 'mi', or 'gi'

    if unit == "ki":
        return int(value * 1024)
    elif unit == "mi":
        return int(value * (1024**2))
    elif unit == "gi":
        return int(value * (1024**3))
    else:
        raise ValueError(f"Unknown memory unit suffix: '{unit}' in '{memory_string}'")