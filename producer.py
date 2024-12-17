from kafka import KafkaProducer
import json
import csv
import os
import base64
import threading
import time

# Fungsi untuk membaca dan mengirimkan data CSV
def send_csv_data(producer, topic, stop_event):
    last_processed_row = 0
    while not stop_event.is_set():  # Menjaga producer tetap berjalan
        with open('./Dataset/amazon_prime_tv_show_dataset.csv', 'r') as file:
            reader = csv.DictReader(file)
            # Skip rows that have already been processed
            for _ in range(last_processed_row):
                next(reader)
            for row in reader:
                if stop_event.is_set():  # Jika event penghentian di-trigger, hentikan pengiriman
                    print("Pengiriman CSV dihentikan.")
                    return
                producer.send(topic, row)
                last_processed_row += 1
                print("Mengirim data CSV:", row)
        time.sleep(1)  # Menunggu 1 detik sebelum memeriksa lagi

# Fungsi untuk membaca dan mengirimkan data gambar
def send_image_data(producer, topic, stop_event):
    processed_images = set()  # Menggunakan set untuk melacak gambar yang telah diproses
    image_folder = './dataset/Image_dataset'
    
    while not stop_event.is_set():  # Menjaga producer tetap berjalan
        for image_name in os.listdir(image_folder):
            if stop_event.is_set():  # Jika event penghentian di-trigger, hentikan pengiriman
                print("Pengiriman gambar dihentikan.")
                return
            if image_name not in processed_images:
                image_path = os.path.join(image_folder, image_name)
                if os.path.isfile(image_path):
                    with open(image_path, 'rb') as image_file:
                        image_data = image_file.read()
                        # Encode image data to base64
                        encoded_image_data = base64.b64encode(image_data).decode('utf-8')
                        
                        # Menentukan nama film dari nama gambar (misalnya, menghilangkan ekstensi .jpg/.png)
                        film_name = os.path.splitext(image_name)[0]  # Mengambil nama file tanpa ekstensi
                        
                        # Send data to Kafka dengan menyertakan 'Name of the show'
                        producer.send(
                            topic, 
                            {
                                'image_name': image_name, 
                                'image_data': encoded_image_data,
                                'Name of the show': film_name  # Nama film diambil dari nama gambar
                            }
                        )
                        processed_images.add(image_name)
                        print(f"Mengirim gambar: {image_name} dengan Nama Film: {film_name}")
        time.sleep(0)  # Menunggu 1 detik sebelum memeriksa lagi

# Inisialisasi Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'big-data-fp10'

# Buat event untuk penghentian thread, jika diperlukan
stop_event = threading.Event()

# Buat thread untuk menjalankan fungsi secara bersamaan
thread_csv = threading.Thread(target=send_csv_data, args=(producer, topic_name, stop_event))
thread_images = threading.Thread(target=send_image_data, args=(producer, topic_name, stop_event))

# Mulai thread
thread_csv.start()
thread_images.start()

# Tunggu kedua thread selesai (selama tidak dihentikan secara manual)
try:
    while True:
        time.sleep(1)  # Program tetap berjalan, thread aktif untuk memonitor data baru
except KeyboardInterrupt:
    # Jika menerima penghentian dari pengguna
    stop_event.set()

# Tunggu kedua thread selesai
thread_csv.join()
thread_images.join()

# Tutup producer
producer.close()
print("Data dari CSV dan folder gambar berhasil dikirim ke Kafka.")
