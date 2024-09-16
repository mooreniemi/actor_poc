import sys
import io

# SETUP - DO NOT REMOVE

# You need to leave this here.
# You can set your own default dummy values, but this is how we load from Rust.
input_data = locals().get("input_data", [])

# Redirect stdout to capture prints
old_stdout = sys.stdout
sys.stdout = io.StringIO()

# YOU EDIT


def process_features(input_data):
    # this must be available in site-packages path you defined
    import numpy as np

    # Ensure input is a NumPy array
    input_data = np.array(input_data)

    # Calculate mean and standard deviation for each feature
    mean = np.mean(input_data, axis=0)
    std = np.std(input_data, axis=0)

    # Avoid division by zero
    std = np.where(std == 0, 1e-8, std)
    print(f"PYTHON - std: {std}, mean: {mean}")

    # Normalize the features
    normalized_data = (input_data - mean) / std

    return normalized_data


# OUTPUTS - DO NOT REMOVE

# Assuming 'input_data' is passed as a variable in the locals
output_data = process_features(input_data)

# Capture printed output, which allows Rust to print it after.
# Unfortunately this means if we errored out we probably didin't see.
captured_output = sys.stdout.getvalue()
