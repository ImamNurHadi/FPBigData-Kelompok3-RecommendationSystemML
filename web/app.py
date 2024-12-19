from flask import Flask, request, jsonify, render_template
import pandas as pd
import psycopg2
from sklearn.preprocessing import MinMaxScaler
from sklearn.ensemble import RandomForestClassifier
import logging
from minio import Minio
import os
import re

# Konfigurasi logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize MinIO client
minio_client = Minio(
    "192.168.242.1:9000",  # Ganti dengan host MinIO Anda
    access_key="Uh396Kv9HYw7Blo2QQFz",  # Ganti dengan access key Anda
    secret_key="3TA2hET1CaJLuOqQDrjQon9zxb3Zn290wrqIuFEm",  # Ganti dengan secret key Anda
    secure=False  # Atur ke True jika menggunakan HTTPS
)

app = Flask(__name__)

def fetch_data_from_postgresql():
    """Mengambil data dari database PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host="localhost",
            dbname="movie",
            user="postgres",
            password="imamnh"
        )
        query = "SELECT * FROM filtered"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        logger.error(f"Error mengambil data dari PostgreSQL: {e}")
        return pd.DataFrame()

def preprocess_data(df):
    """Melakukan preprocessing data."""
    # Ganti NaN pada kolom 'age_of_viewers' dengan 'All'
    df['age_of_viewers'] = df['age_of_viewers'].fillna('All')
    
    # Hapus duplikasi berdasarkan nama film
    df = df.drop_duplicates(subset='name_of_show')  
    
    return df

def clean_filename(name):
    """Clean file or folder names to make them filesystem-safe."""
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = name.replace("'", "")
    name = name.replace(":", "_")
    return name

def train_model(df):
    """Melatih model Machine Learning."""
    df['age_of_viewers'] = df['age_of_viewers'].replace('All', 0)  # Ganti 'All' dengan 0 atau nilai yang sesuai
    
    df['age_of_viewers'] = df['age_of_viewers'].astype(int)
    features = df[['age_of_viewers', 'imdb_rating']]
    labels = df['genre']
    
    scaler = MinMaxScaler()
    features_scaled = scaler.fit_transform(features)
    
    model = RandomForestClassifier()
    model.fit(features_scaled, labels)
    
    return model, scaler

def get_recommendations(user_genre, user_rating, user_age, data, model, scaler):
    """Mencari rekomendasi berdasarkan input pengguna."""
    user_rating = float(user_rating)
    user_age = int(user_age)
    
    # Filter data yang sesuai dengan input pengguna
    filtered_data = data[
        (data['genre'].str.contains(user_genre, case=False, na=False)) & 
        (data['imdb_rating'] >= user_rating) &
        (data['age_of_viewers'] <= user_age)
    ]
    
    if not filtered_data.empty:
        return filtered_data
    
    # Jika tidak ada film yang sesuai, cari rekomendasi alternatif
    logger.warning("Tidak ada film yang sesuai. Memberikan rekomendasi alternatif.")
    user_features = pd.DataFrame([[user_age, user_rating]], columns=['age_of_viewers', 'imdb_rating'])
    user_features_scaled = scaler.transform(user_features)
    
    predictions = model.predict(user_features_scaled)
    alternative_genre = predictions[0]
    
    alternative_data = data[data['genre'].str.contains(alternative_genre, case=False, na=False)]
    return alternative_data

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/recommend', methods=['POST'])
def recommend():
    user_genre = request.form['genre']
    user_rating = request.form['rating']
    user_age = request.form['age']
    
    # Ambil data dari PostgreSQL dan latih model
    data = fetch_data_from_postgresql()
    if data.empty:
        return jsonify({"error": "Tidak ada data ditemukan."})
    
    data = preprocess_data(data)
    model, scaler = train_model(data)
    
    # Cari rekomendasi
    recommendations = get_recommendations(user_genre, user_rating, user_age, data, model, scaler)
    
    if recommendations.empty:
        return jsonify({"message": "Tidak ada rekomendasi yang ditemukan."})
    
    # Menampilkan hasil rekomendasi
    recommendations_list = recommendations[['name_of_show', 'genre', 'imdb_rating', 'age_of_viewers']].head(8).to_dict(orient='records')
    
    # Render hasil rekomendasi ke template
    return render_template('index.html', recommendations=recommendations_list)

@app.route('/details/<movie_name>', methods=['GET'])
def movie_details(movie_name):
    bucket_name = "movie"
    unfiltered_folder = "unfiltered"
    try:
        # Cari file CSV terkait di MinIO
        objects = minio_client.list_objects(bucket_name, prefix=unfiltered_folder, recursive=True)
        for obj in objects:
            if obj.object_name.endswith('.csv'):
                # Unduh file CSV
                local_temp_path = os.path.join('temp', 'movie_details.csv')
                minio_client.fget_object(bucket_name, obj.object_name, local_temp_path)

                # Baca file CSV untuk mencari detail film
                df = pd.read_csv(local_temp_path, encoding='utf-8')
                movie_details = df[df['Name of the show'].str.contains(movie_name, case=False)]

                if not movie_details.empty:
                    # Ambil detail film sebagai dictionary
                    movie = movie_details.iloc[0].to_dict()

                    # Cari file gambar terkait di folder film
                    film_folder = f"{unfiltered_folder}/{clean_filename(movie_name.replace(' ', '_'))}"
                    image_url = None

                    # Cek gambar dengan ekstensi .jpg
                    for obj in minio_client.list_objects(bucket_name, prefix=film_folder, recursive=True):
                        if obj.object_name.endswith('.jpg'):
                            # Buat URL gambar tanpa presigned URL
                            image_url = f'http://192.168.242.1:9000/{bucket_name}/{obj.object_name}'
                            break

                    # Tambahkan URL gambar ke movie
                    movie['image_url'] = image_url
                    return render_template('details.html', movie=movie)

        return render_template('details.html', movie=None, message=f"Detail untuk {movie_name} tidak ditemukan.")

    except Exception as e:
        logger.error(f"Error saat mengambil detail film: {e}")
        return render_template('details.html', movie=None, message="Terjadi kesalahan dalam pengambilan detail film.")

if __name__ == '__main__':
    app.run(debug=True)