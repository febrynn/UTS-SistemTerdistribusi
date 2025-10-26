 UTS Sistem Terdistribusi – Pub Sub Log Aggregator

 <!-- Penjelasan  -->
 Proyek ini merupakan implementasi Log Aggregator Terdistribusi berbasis arsitektur Publish–Subscribe (Pub/Sub)
Sistem terdiri dari dua komponen utama:
- Publisher → mengirim event log ke sistem.
- Aggregator → menerima event, melakukan deduplikasi, menyimpan log ke database, dan menyediakan statistik melalui FastAPI.  

Seluruh komponen dijalankan secara terisolasi menggunakan Docker Compose

Tools yang digunakan 
- Python 3.13  
- FastAPI  
- Asyncio  
- SQLite (Dedup Store)  
- Docker & Docker Compose  
- Pytest (Testing Framework)

<!-- Cara menjalankan  -->

1. Build dan jalankan layanan menggunakan Docker Compose
   Pada terminal
   'docker compose up --build'
2. Akses 'http://localhost:8000/docs'


<!-- Laporan  -->

Link drive : https://drive.google.com/drive/folders/1bx9XQF-wfmBmGmMcqKw_20m4hVjca8Ew?usp=drive_link
Link YT : https://youtu.be/WWYCktIy16Y?si=A6d-AX2Zi8fePXwD
