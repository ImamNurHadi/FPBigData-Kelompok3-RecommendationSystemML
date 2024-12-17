import pandas as pd
import psycopg2
from sklearn.preprocessing import MinMaxScaler
from sklearn.ensemble import RandomForestClassifier
import logging

# Konfigurasi logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    df = df.dropna()  # Hapus baris dengan nilai kosong
    df = df.drop_duplicates(subset='name_of_show')  # Hapus duplikasi berdasarkan nama film
    return df

def train_model(df):
    """Melatih model Machine Learning."""
    df['age_of_viewers'] = df['age_of_viewers'].fillna(0).astype(int)
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

if __name__ == "__main__":
    print("Program dimulai...")

    try:
        print("Mengambil data dari PostgreSQL...")
        data = fetch_data_from_postgresql()
        
        if data.empty:
            print("Tidak ada data yang ditemukan di tabel 'filtered'.")
        else:
            print("Data berhasil diambil.")
            
            print("Melakukan preprocessing data...")
            data = preprocess_data(data)
            print("Preprocessing selesai.")
            
            print("Melatih model Machine Learning...")
            model, scaler = train_model(data)
            
            # Input user
            user_genre = input("Masukkan genre (misal: Romance): ")
            user_rating = input("Masukkan rating (misal: 9.0): ")
            user_age = input("Masukkan usia Anda (misal: 16): ")
            
            print("Mencari rekomendasi film...")
            recommendations = get_recommendations(user_genre, user_rating, user_age, data, model, scaler)
            if recommendations.empty:
                print("Tidak ada rekomendasi yang ditemukan.")
            else:
                print("\nRekomendasi untuk Anda:")
                print(recommendations[['name_of_show', 'genre', 'imdb_rating']].head(8))
    
    except Exception as e:
        print(f"Terjadi kesalahan: {e}")
