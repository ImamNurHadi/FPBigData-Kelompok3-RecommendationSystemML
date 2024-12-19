from flask import Flask, request, jsonify, render_template
import pandas as pd
import psycopg2
from sklearn.preprocessing import MinMaxScaler
from sklearn.ensemble import RandomForestClassifier
import logging

# Konfigurasi logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

if __name__ == '__main__':
    app.run(debug=True)