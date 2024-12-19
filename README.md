# Final Project Big Data dan Data Lakehouse

| No | Nama                     | NRP        |
|----|--------------------------|------------|
| 1  | Gilang Raya              | 5027221045 |
| 2  | Imam Nurhadi             | 5027221046 |
| 3  | Zulfa Hafizh Kusuma      | 5027221038 |
| 4  | Muhammad Rifqi Oktaviansyah | 5027221067 |
| 5  | Rizki Ramadhani          | 5027221013 |


## Kafka Producer: Mengirimkan Data CSV dan Gambar
1. **Producer Data CSV:** Membaca data dari file CSV dan mengirim setiap baris ke topik Kafka.
2. **Producer Data Gambar:** Membaca gambar dari folder, meng-encode ke Base64, dan mengirimnya ke topik Kafka dengan metadata.
3. **Multi-threading:** Producer data CSV dan gambar berjalan secara bersamaan.
4. **Penghentian Aman:** Mendukung penghentian thread secara aman menggunakan event.

## Struktur File
```
project/
├── Dataset/
│   ├── amazon_prime_tv_show_dataset.csv
├── dataset/
│   ├── Image_dataset/
│       ├── image1.jpg
│       ├── image2.png
├── producer.py
```

## Prasyarat
1. **Python**: Versi 3.6+
2. **Kafka**: Kafka broker yang berjalan (localhost:9092 pada contoh ini)
3. **Library Python**:
   - kafka-python
   - json
   - csv
   - os
   - base64
   - threading
   - time

Instal library Python yang diperlukan:
```bash
pip install kafka-python
```

## Penjelasan Kode

### 1. Producer Data CSV
Membaca baris dari file CSV dan mengirimkannya ke Kafka.
```python
def send_csv_data(producer, topic, stop_event):
    last_processed_row = 0
    while not stop_event.is_set():
        with open('./Dataset/amazon_prime_tv_show_dataset.csv', 'r') as file:
            reader = csv.DictReader(file)
            for _ in range(last_processed_row):
                next(reader)
            for row in reader:
                if stop_event.is_set():
                    print("Pengiriman CSV dihentikan.")
                    return
                producer.send(topic, row)
                last_processed_row += 1
                print("Mengirim data CSV:", row)
        time.sleep(1)
```

### 2. Producer Data Gambar
Membaca file gambar, meng-encode ke Base64, dan mengirimkannya ke Kafka dengan metadata.
```python
def send_image_data(producer, topic, stop_event):
    processed_images = set()
    image_folder = './dataset/Image_dataset'

    while not stop_event.is_set():
        for image_name in os.listdir(image_folder):
            if stop_event.is_set():
                print("Pengiriman gambar dihentikan.")
                return
            if image_name not in processed_images:
                image_path = os.path.join(image_folder, image_name)
                if os.path.isfile(image_path):
                    with open(image_path, 'rb') as image_file:
                        image_data = image_file.read()
                        encoded_image_data = base64.b64encode(image_data).decode('utf-8')
                        film_name = os.path.splitext(image_name)[0]

                        producer.send(
                            topic, 
                            {
                                'image_name': image_name, 
                                'image_data': encoded_image_data,
                                'Name of the show': film_name
                            }
                        )
                        processed_images.add(image_name)
                        print(f"Mengirim gambar: {image_name} dengan Nama Film: {film_name}")
        time.sleep(1)
```

### 3. Inisialisasi Kafka Producer
Membuat Kafka producer untuk men-serialize data sebagai JSON.
```python
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
```

### 4. Program Utama
Mengatur threading dan penghentian aman.
```python
topic_name = 'big-data-fp10'
stop_event = threading.Event()

thread_csv = threading.Thread(target=send_csv_data, args=(producer, topic_name, stop_event))
thread_images = threading.Thread(target=send_image_data, args=(producer, topic_name, stop_event))

thread_csv.start()
thread_images.start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    stop_event.set()

thread_csv.join()
thread_images.join()
producer.close()
print("Data dari CSV dan folder gambar berhasil dikirim ke Kafka.")
```

## Cara Menjalankan
1. Pastikan Kafka berjalan pada `localhost:9092`.
2. Letakkan file CSV di folder `Dataset/`.
3. Letakkan file gambar di folder `dataset/Image_dataset/`.
4. Jalankan skrip producer:
```bash
python producer.py
```
5. Untuk menghentikan skrip, tekan `Ctrl+C`. Ini akan menghentikan semua thread secara aman.

## Contoh Output
```
Mengirim data CSV: {'Title': 'Example Show', 'Genre': 'Drama'}
Mengirim gambar: image1.jpg dengan Nama Film: image1
Mengirim gambar: image2.png dengan Nama Film: image2
```

## Catatan
- Pastikan jalur folder dan konfigurasi topik Kafka sudah benar.
- Sesuaikan interval waktu (`time.sleep()`) sesuai kebutuhan.
- Tambahkan penanganan kesalahan untuk meningkatkan keandalan di lingkungan produksi.

## Filtering

`

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
    logger = logging.getLogger(__name__)

    class FolderScanner:
    def __init__(self, base_folder, minio_client, exclude_file='path_film.csv', filtered_folder='filtered'):
        self.base_folder = base_folder
        self.exclude_file = exclude_file
        self.filtered_folder = filtered_folder
        self.minio_client = minio_client
        
        # Path untuk folder 'filtered'
        self.filtered_path = os.path.join(self.base_folder, self.filtered_folder)
        
        # Jika folder 'filtered' belum ada, buat folder tersebut
        if not os.path.exists(self.filtered_path):
            os.makedirs(self.filtered_path)
  
        # Menyimpan state file yang telah diproses untuk menghindari pemrosesan ulang
        self.processed_files = set()

### Memeriksa folder dengan nama unfiltered pada Minio
Pemeriksaan dilakukan pada bucket movie, dengan menginisiasi terlebih dahulu minio kita.

    def scan_and_process(self):
        """Memindai folder 'unfiltered' di MinIO dan memproses file CSV."""
        bucket_name = 'movie'
        unfiltered_folder = 'unfiltered'
        
        try:
            # List file di folder unfiltered MinIO
            objects = self.minio_client.list_objects(bucket_name, prefix=unfiltered_folder, recursive=True)
            for obj in objects:
                if obj.object_name.endswith('.csv') and obj.object_name != self.exclude_file:
                    file_path = obj.object_name
                    if file_path not in self.processed_files:
                        logger.info(f"Memproses file baru: {file_path}")
                        self.process_file(bucket_name, file_path)
                        self.processed_files.add(file_path)
        except S3Error as e:
            logger.error(f"Error mengakses MinIO: {e}")

### Mengubah nilai ALL menjadi null dan dikembalikan nilai umur menjadi integer yaitu 0. 

    def convert_age_to_int(self, age_value):
        """Mengonversi 'Age of viewers' menjadi nilai integer atau None untuk nilai yang tidak valid."""
        if age_value in ['All', 'All ages', 'NA', ''] or pd.isna(age_value):
            return None  # Mengonversi nilai tidak valid menjadi None
        try:
            return int(age_value.replace('+', '').strip())
        except ValueError:
            return None

### Memproses file pada minio untuk dikirim ke POSTGRESQL
proses dilakukan dengan mengecek kolom pada csv

    def process_file(self, bucket_name, file_path):
        """Memproses file CSV dan menyimpan hasilnya di folder 'filtered/{file_name}/{file_name}.csv'."""
        try:
            # Mengunduh file CSV dari MinIO
            local_temp_path = os.path.join(self.base_folder, 'temp_file.csv')
            self.minio_client.fget_object(bucket_name, file_path, local_temp_path)
            logger.info(f"File {file_path} berhasil diunduh.")

            # Abaikan file kosong
            if os.stat(local_temp_path).st_size == 0:
                logger.warning(f"File {file_path} kosong. Melewati pemrosesan.")
                return
            
            # Membaca file CSV dengan encoding dan delimiter
            df = pd.read_csv(local_temp_path, encoding='utf-8', delimiter=',')
            logger.info(f"Membaca file: {file_path}")
            
            # Mengecek apakah kolom yang dibutuhkan ada di CSV
            required_columns = ['Name of the show', 'Language', 'Genre', 'IMDb rating', 'Age of viewers']
            if all(col in df.columns for col in required_columns):
                # Menyaring hanya kolom yang dibutuhkan
                filtered_df = df[required_columns]

                # Memformat kolom "Name of the show" ke title case menggunakan .loc[]
                filtered_df.loc[:, 'Name of the show'] = filtered_df['Name of the show'].str.title()
                
                # Mengonversi kolom 'Age of viewers' menjadi integer menggunakan .loc[] dan fungsi convert_age_to_int
                filtered_df.loc[:, 'Age of viewers'] = filtered_df['Age of viewers'].apply(self.convert_age_to_int)
                
                # Memasukkan data ke PostgreSQL
                self.save_to_postgresql(filtered_df)
                
                # Menyimpan hasil ke file CSV di folder 'filtered/{file_name}/{file_name}.csv'
                file_name = os.path.splitext(os.path.basename(file_path))[0]
                target_folder = os.path.join(self.filtered_path, file_name)
                if not os.path.exists(target_folder):
                    os.makedirs(target_folder)
                
                filtered_file_path = os.path.join(target_folder, f"{file_name}.csv")
                filtered_df.to_csv(filtered_file_path, index=False, encoding='utf-8')
                logger.info(f"Hasil filter telah disimpan ke {filtered_file_path}.")
                
                # Upload ke MinIO
                self.upload_to_minio(filtered_file_path, file_name)
                
            else:
                logger.warning(f"File {file_path} tidak memiliki kolom yang diperlukan.")
            
            # Hapus file sementara setelah pemrosesan
            os.remove(local_temp_path)
        except pd.errors.EmptyDataError:
            logger.error(f"File {file_path} kosong atau tidak dapat dibaca.")
        except Exception as e:
            logger.error(f"Error membaca {file_path}: {e}")

### Menyimpan kolom yang sudah ditentukan ke PostgreSQL sebagai data yang bersih (Filtered)

    def save_to_postgresql(self, df):
        """Menyimpan DataFrame ke PostgreSQL."""
        try: 
            # Koneksi ke PostgreSQL
            conn = psycopg2.connect(
                host="localhost",  # Ganti dengan host PostgreSQL Anda
                dbname="movie",  # Ganti dengan nama database Anda
                user="postgres",  # Ganti dengan username PostgreSQL Anda
                password="imamnh",  # Ganti dengan password PostgreSQL Anda
            )
            cursor = conn.cursor()

            # Menyimpan data ke PostgreSQL
            for index, row in df.iterrows():
                cursor.execute(
                    "INSERT INTO filtered (name_of_show, genre, imdb_rating, age_of_viewers) VALUES (%s, %s, %s, %s)",
                    (row['Name of the show'], row['Genre'], row['IMDb rating'], row['Age of viewers'])
                )
            
            # Commit perubahan dan menutup koneksi
            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"Data berhasil disimpan ke database PostgreSQL.")
        except Exception as e:
            logger.error(f"Error menyimpan data ke database: {e}")
            
### Upload ke Minio sebagai Data Lakehouse untuk data Filtered
    
    def upload_to_minio(self, local_file_path, file_name):
        """Mengupload file ke MinIO."""
        try:
            bucket_name = 'movie'
            target_path = f"filtered/{file_name}/{file_name}.csv"
            self.minio_client.fput_object(bucket_name, target_path, local_file_path)
            logger.info(f"File berhasil diupload ke MinIO di {target_path}")
            
            # Hapus file lokal setelah diupload
            os.remove(local_file_path)
        except S3Error as e:
            logger.error(f"Error mengupload file ke MinIO: {e}")

### Melakukan scanning untuk setiap pemindahan
Hal ini dilakukan untuk monitoring setiap proses perpindahan data.

    def start_scanning(minio_client, base_folder='Lakehouse', scan_interval=5):
        """Memulai pemindaian folder secara terus-menerus."""
        scanner = FolderScanner(base_folder, minio_client)
        logger.info("Pemindaian dimulai. Tekan Ctrl+C untuk menghentikan.")
        try:
            while True:
                scanner.scan_and_process()
                time.sleep(scan_interval)  # Tunggu beberapa detik sebelum memindai lagi
        except KeyboardInterrupt:
            logger.info("Pemindaian dihentikan.")
            
### Inisialisasi minio Client

    # Inisialisasi MinIO client
    minio_client = Minio(
        "192.168.242.1:9000",  # Ganti dengan host MinIO Anda
        access_key="Uh396Kv9HYw7Blo2QQFz",  # Ganti dengan access key Anda
        secret_key="3TA2hET1CaJLuOqQDrjQon9zxb3Zn290wrqIuFEm",  # Ganti dengan secret key Anda
        secure=False  # Atur ke True jika menggunakan HTTPS
    )

### Main 

    # Memulai pemindaian folder
    if __name__ == "__main__":
        start_scanning(minio_client)

`


# Web index.html

Halaman web sederhana untuk memberikan rekomendasi film berdasarkan input pengguna.

## Struktur Dasar HTML
```
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Rekomendasi Film</title>
</head>
<body>
  <!-- Konten utama ada di sini -->
</body>
</html>
```
Struktur dasar HTML untuk halaman web.

## Latar Belakang Animasi
```
<div class="background">
  <div class="background-row">
    <img src="Thumbnail1.png" alt="Thumbnail 1">
    <img src="Thumbnail2.png" alt="Thumbnail 2">
    <img src="Thumbnail3.png" alt="Thumbnail 3">
  </div>
</div>
```
Menampilkan gambar film sebagai animasi latar belakang bergerak.
## Header
```
<header>
  <h1>Rekomendasi Film</h1>
</header>
```
Judul halaman di bagian atas.

## Formulir Pencarian

```
<div class="search-section">
  <form action="/recommend" method="post">
    <label for="genre">Genre:</label>
    <input type="text" id="genre" name="genre" required>
    <label for="rating">Rating Minimum:</label>
    <input type="text" id="rating" name="rating" required>
    <label for="age">Usia:</label>
    <input type="text" id="age" name="age" required>
    <button type="submit">Cari Rekomendasi</button>
  </form>
</div>
```
Formulir untuk pengguna memasukkan preferensi pencarian film.

## Hasil Rekomendasi
```
<div class="results">
  {% for recommendation_row in recommendations|batch(3) %}
  <div class="row">
    {% for recommendation in recommendation_row %}
    <div class="card">
      <img src="{{ recommendation.poster_url }}" alt="Poster">
      <h3>{{ recommendation.name_of_show }}</h3>
      <p>Genre: {{ recommendation.genre }}</p>
      <p>Rating: {{ recommendation.imdb_rating }}</p>
      <p>Usia: {{ recommendation.age_of_viewers }}</p>
      <button>Tambah ke Favorit</button>
    </div>
    {% endfor %}
  </div>
  {% endfor %}
</div>
```
Menampilkan daftar rekomendasi dalam format card.
## Footer
```
<footer>
  <a href="#">Big Data</a>
  <a href="#">Kelompok</a>
</footer>
```
# CSS
```
body {
  font-family: Arial, sans-serif;
  color: #fff;
  background-color: #1a1a1a;
  margin: 0;
  padding: 0;
}

.background {
  position: fixed;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  z-index: -1;
}
```
Mengatur tema gelap, animasi, dan tata letak responsif.
