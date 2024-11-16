import argparse
import numpy as np
import pandas as pd
from sklearn.tree import DecisionTreeClassifier
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.base import BaseEstimator, TransformerMixin
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType
import onnx
from onnx import checker
from datetime import datetime

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Export Decision Tree with custom preprocessing to ONNX.")
parser.add_argument("--no-preprocess", action="store_true", help="Disable preprocessing in the ONNX model.")
parser.add_argument("--f", help="(vs code interactive argument set, ignore)")
args = parser.parse_args()

# Generate synthetic data for a classification task
n_samples = 100
X = pd.DataFrame({
    "timestamp": [datetime(2022, 1, 1).timestamp() + i * 3600 for i in range(n_samples)],  # Hourly timestamps
    "string_feature": [f"string_{i}" for i in range(n_samples)],                         # String data
    "numeric_feature1": np.random.rand(n_samples),                                       # Numeric feature 1
    "numeric_feature2": np.random.rand(n_samples),                                       # Numeric feature 2
    "categorical_feature": np.random.choice(["A", "B", "C"], size=n_samples),            # Categorical feature
})
y = np.random.choice([0, 1], size=n_samples)  # Binary classification target


# Custom transformer to process strings
class StringLengthTransformer(BaseEstimator, TransformerMixin):
    def fit(self, X, y=None):
        return self

    def transform(self, X):
        # Ensure the input is a 2D array or DataFrame
        if isinstance(X, pd.Series):
            X = X.to_frame()  # Convert Series to DataFrame
        return X.applymap(len).to_numpy(dtype=np.float32)  # Compute string lengths


# Preprocessing steps (optional, based on argument)
if not args.no_preprocess:
    print("Including preprocessing in the ONNX model...")
    preprocessor = ColumnTransformer(
        transformers=[
            ("timestamp", StandardScaler(), ["timestamp"]),  # Scale timestamps
            ("string_length", StringLengthTransformer(), ["string_feature"]),  # String length processing
            ("num", StandardScaler(), ["numeric_feature1", "numeric_feature2"]),  # Scale numeric features
            ("cat", OneHotEncoder(), ["categorical_feature"]),  # One-hot encode categorical features
        ]
    )
else:
    print("Preprocessing excluded from the ONNX model.")
    preprocessor = "passthrough"  # Skips preprocessing

# Define the decision tree model
classifier = DecisionTreeClassifier(max_depth=3, random_state=42)

# Combine preprocessing and model into a pipeline
pipeline = Pipeline(steps=[("preprocessor", preprocessor), ("classifier", classifier)])

# Fit the pipeline
pipeline.fit(X, y)

# Path to save the ONNX model
onnx_path = "../data/decision_tree_with_custom_pipeline.onnx"

# Specify the input type for the model
initial_type = [
    ("timestamp", FloatTensorType([None, 1])),
    ("string_feature", FloatTensorType([None, 1])),  # String length after transformation
    ("numeric_feature1", FloatTensorType([None, 1])),
    ("numeric_feature2", FloatTensorType([None, 1])),
    ("categorical_feature", FloatTensorType([None, 1])),
]

# Attempt to convert the pipeline to ONNX
try:
    onnx_model = convert_sklearn(
        pipeline,
        initial_types=initial_type,
        target_opset=9,  # Ensure compatibility with ONNX Runtime
        options={"zipmap": False},  # Disable zipmap for compatibility
    )
    # Save the ONNX model to a file
    with open(onnx_path, "wb") as f:
        f.write(onnx_model.SerializeToString())
    print(f"Pipeline with decision tree model has been converted to ONNX and saved as {onnx_path}")

    # Validate the ONNX model
    model = onnx.load(onnx_path)
    checker.check_model(model)
    print("The ONNX model is valid.")
except Exception as e:
    print(f"Conversion to ONNX failed: {e}")