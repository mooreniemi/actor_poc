import onnx

# Load the ONNX model
onnx_path = "../data/decision_tree_with_pipeline.onnx"
model = onnx.load(onnx_path)

# Print all nodes in the graph
print("ONNX Graph Nodes:")
for node in model.graph.node:
    print(f"OpType: {node.op_type}, Name: {node.name}")