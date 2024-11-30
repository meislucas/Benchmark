import csv
import os
from faker import Faker

# Initialize Faker
fake = Faker()

# Define the sizes of the datasets
sizes = [100, 10000, 1000000, 2000000]

# Define the directory to save the datasets
output_dir = '../data'
os.makedirs(output_dir, exist_ok=True)

# Define the headers for the CSV files
headers = ['product_id', 'event_type', 'price']

# Function to generate a single record
def generate_record():
    return {
        'product_id': fake.uuid4(),
        'event_type': fake.random_element(elements=('purchase', 'refund', 'click')),
        'price': round(fake.random_number(digits=5, fix_len=True) / 100, 2)
    }

# Generate datasets
for size in sizes:
    file_path = os.path.join(output_dir, f'data_{size}.csv')
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        for _ in range(size):
            writer.writerow(generate_record())

print("CSV datasets created successfully.")