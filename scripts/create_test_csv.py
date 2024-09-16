import csv

# File path for the CSV file
csv_file = "/tmp/sample_data.csv"

# Sample data to write to the CSV (5 columns with floating point values)
data = [
    [1.1, 2.2, 3.3, 4.4, 5.5],
    [6.6, 7.7, 8.8, 9.9, 10.1],
    [11.2, 12.3, 13.4, 14.5, 15.6],
    [16.7, 17.8, 18.9, 19.0, 20.1],
    [21.2, 22.3, 23.4, 24.5, 25.6],
]

# Write data to CSV
with open(csv_file, mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerows(data)

print(f"CSV file '{csv_file}' created successfully.")
