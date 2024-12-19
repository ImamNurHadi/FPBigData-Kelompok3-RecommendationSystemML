import subprocess
import time

def main():
    scripts = ["producer.py", "consumer.py", "filtering.py", "web/app.py"]

    processes = []

    for script in scripts:
        print(f"Menjalankan {script} di terminal baru...")
        # Jeda 3 detik sebelum menjalankan script berikutnya
        time.sleep(3)
        # Jalankan setiap file Python di terminal baru
        process = subprocess.Popen(
            ["python", script],
            creationflags=subprocess.CREATE_NEW_CONSOLE
        )
        processes.append(process)

    # Loop untuk menjaga agar program tidak berhenti
    try:
        while True:
            # Opsional: Periksa apakah semua proses masih berjalan
            time.sleep(1)
    except KeyboardInterrupt:
        print("Program dihentikan oleh pengguna. Menutup semua proses...")
        for process in processes:
            process.terminate()  # Menghentikan proses
        print("Semua proses telah dihentikan.")

if __name__ == "__main__":
    main()