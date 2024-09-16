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
    """
    Function to process the input features.
    This example simply adds 1 to each feature value.
    Replace this logic with whatever your feature processing requires.
    """
    # Assume input_data is a list of numbers
    processed_data = [x + 1 for x in input_data]
    print(
        f"PYTHON PYTHON PYTHON - Processed features: {processed_data}"
    )  # This will now be captured
    return processed_data


# OUTPUTS - DO NOT REMOVE

# Assuming 'input_data' is passed as a variable in the locals
output_data = process_features(input_data)

# Capture printed output, which allows Rust to print it after.
# Unfortunately this means if we errored out we probably didin't see.
captured_output = sys.stdout.getvalue()
