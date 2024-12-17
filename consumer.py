from kafka import KafkaConsumer
import json
import os
import csv
import re
import math
import base64
from minio import Minio
from minio.error import S3Error

# Kafka consumer initialization
consumer = KafkaConsumer(
    'big-data-fp10', 
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

# Initialize MinIO client
minio_client = Minio(
    "192.168.242.1:9000",  # Ganti dengan host MinIO Anda
    access_key="Uh396Kv9HYw7Blo2QQFz",  # Ganti dengan access key Anda
    secret_key="3TA2hET1CaJLuOqQDrjQon9zxb3Zn290wrqIuFEm",  # Ganti dengan secret key Anda
    secure=False  # Atur ke True jika menggunakan HTTPS
)

def clean_filename(name):
    """Clean file or folder names to make them filesystem-safe."""
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = name.replace("'", "")
    name = name.replace(":", "_")
    return name

def remove_nan(data, default_value="N/A"):
    """Replaces NaN values in a dictionary with a default value."""
    return {key: (value if not isinstance(value, float) or not math.isnan(value) else default_value)
            for key, value in data.items()}

def upload_to_minio(file_path, bucket_name, object_name):
    """Upload file to MinIO."""
    try:
        minio_client.fput_object(bucket_name, object_name, file_path)
        print(f"File '{file_path}' uploaded to MinIO as '{object_name}' in bucket '{bucket_name}'.")
    except S3Error as e:
        print(f"Error uploading file to MinIO: {e}")

# Base folder for saving data
base_folder = 'Lakehouse'
if not os.path.exists(base_folder):
    os.makedirs(base_folder)

# Path for CSV to store metadata
path_file = os.path.join(base_folder, 'path_film.csv')

# Read existing paths to avoid duplicates
existing_paths = set()
if os.path.exists(path_file):
    with open(path_file, mode='r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  
        for row in reader:
            existing_paths.add(row[1])  # Add the path (second column) to the set

# Prepare path_film.csv if it doesn't exist
if not os.path.exists(path_file):
    with open(path_file, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Film Name', 'CSV File Path'])  # Write header row

# Kafka consumer loop
for message in consumer:
    show_data = message.value 

    if 'image_name' in show_data and 'image_data' in show_data:
        # Handle image data
        film_name = clean_filename(show_data.get('Name of the show', 'Unknown_Show').replace(" ", "_"))
        film_folder = os.path.join(base_folder, 'unfiltered', film_name)  # Store in the 'unfiltered' folder
        if not os.path.exists(film_folder):
            os.makedirs(film_folder)

        image_name = clean_filename(show_data['image_name'])
        image_path = os.path.join(film_folder, image_name)

        # Decode base64 image data and save as file
        with open(image_path, 'wb') as image_file:
            image_file.write(base64.b64decode(show_data['image_data']))

        print(f"Gambar '{image_name}' berhasil disimpan di {image_path}")

        # Upload the image to MinIO (under 'unfiltered' folder)
        upload_to_minio(image_path, 'movie', f'unfiltered/{film_name}/{image_name}')

    else:
        # Handle CSV data
        show_data = remove_nan(show_data)
        film_name = clean_filename(show_data.get('Name of the show', 'Unknown_Show').replace(" ", "_"))
        film_folder = os.path.join(base_folder, 'unfiltered', film_name)  # Store in the 'unfiltered' folder

        if not os.path.exists(film_folder):
            os.makedirs(film_folder)

        file_path = os.path.join(film_folder, f"{film_name}.csv")

        if file_path in existing_paths:
            continue

        with open(file_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=show_data.keys())
            writer.writeheader()
            writer.writerow(show_data)

        with open(path_file, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([film_name, file_path])

        existing_paths.add(file_path)
        print(f"Data untuk film '{show_data['Name of the show']}' berhasil disimpan di {file_path}")

        # Upload the CSV to MinIO (under 'unfiltered' folder)
        upload_to_minio(file_path, 'movie', f'unfiltered/{film_name}/{film_name}.csv')
        print(f"Path untuk film '{show_data['Name of the show']}' disimpan di {path_file}")
