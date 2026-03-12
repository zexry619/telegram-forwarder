# Telegram Forwarder

Aplikasi ini meneruskan berbagai media dari semua chat (kecuali yang Anda kecualikan) ke satu chat tujuan.

## Fitur Utama
- Login dan manajemen sesi akun Telegram.
- Default route untuk mode lama: satu target utama plus pengecualian.
- Multi-route: satu akun bisa punya banyak aturan sumber -> tujuan yang berbeda.
- Filter tipe media yang diteruskan (foto, video, dokumen atau semua).
- Otomatis menghindari duplikasi pesan.
- Penjadwalan otomatis untuk menyalakan atau menghentikan worker pada jam tertentu.
- Menu `Migrasi Media` di dalam bot untuk menyalin media dari satu grup/channel ke grup/channel lain (fallback re-upload bila forward dibatasi).

Semua pengaturan dapat dilakukan melalui menu bot setelah Anda menjalankan aplikasi.

## Alur Pengguna Baru

- Ketik `/start` ke bot.
- Jika akun Telegram belum pernah login, bot akan menampilkan onboarding dan mewajibkan login terlebih dahulu.
- Masukkan nomor Telegram, lalu kode verifikasi dengan format `1_2_3_4_5`.
- Jika akun memakai 2FA, masukkan password Telegram Anda.
- Setelah login berhasil, semua fitur utama tersedia dari satu menu: default route, kelola routes, migrasi, pengaturan, lalu jalankan worker.

## Multi-Route

- `Default Route` dipakai untuk kompatibilitas dengan flow lama.
- Menu `Kelola Routes` dipakai untuk membuat aturan tambahan.
- Setiap route bisa punya nama sendiri, sumber, tujuan, filter media multi-pilihan, daftar pengecualian, opsi re-upload, dan status aktif/nonaktif sendiri.
- Worker akan mengecek semua route aktif dan menjalankan route yang cocok dengan pesan masuk.

## Instalasi

1. **Clone repository ini:**
   ```bash
   git clone https://github.com/zexry619/telegram-forwarder.git
   cd telegram-forwarder
   ```

2. **Buat Virtual Environment (opsional tapi disarankan):**
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # Untuk Linux/Mac
   # atau
   venv\Scripts\activate     # Untuk Windows
   ```

3. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## Konfigurasi

1. **Buat file `.env`:**
   Salin file contoh konfigurasi menjadi `.env`:
   ```bash
   cp .env.example .env
   ```

2. **Edit `.env`:**
   Isi variabel penting seperti:
   - `TELEGRAM_API_ID` & `TELEGRAM_API_HASH`: Dapatkan dari [my.telegram.org](https://my.telegram.org).
   - `BOT_TOKEN`: Dapatkan dari [@BotFather](https://t.me/BotFather).
   - `ADMIN_USER_IDS`: ID Telegram Anda (koma separated jika banyak).
   - Sesuaikan konfigurasi lain sesuai kebutuhan.

## Menjalankan Bot

Jalankan bot menggunakan perintah:

```bash
python main.py
```


## Hemat Disk / Manajemen Cache
- Direct-forward first: Bot selalu mencoba forward langsung terlebih dahulu. Unduhan penuh hanya terjadi jika forward ditolak dan re-upload diaktifkan, sehingga forward yang sukses tidak tertunda.
- `Eager download cache`: Opsi ini tidak lagi menyebabkan unduhan sebelum forward; cache hanya dimanfaatkan saat proses re-upload diperlukan.
- `CACHE_TTL_HOURS` (env): Lama waktu cache disimpan sebelum dihapus otomatis (default 24 jam).
- `MAX_CACHE_DISK_MB` (env): Batas ukuran total folder `downloads`. Jika terlewati, file tertua akan dihapus hingga kembali di bawah batas. Biarkan kosong untuk menonaktifkan kuota.
- `MAX_UPLOAD_SIZE_MB` (env): Batas ukuran file yang akan diunduh/unggah (default 2048 MB).

Bot menjalankan pembersihan cache setiap jam. Anda juga bisa menjalankan pembersihan manual:

```
python utils/cleanup.py
```

Catatan: Cache tersimpan di folder `downloads/<user_id>`. File yang dipakai untuk re-upload akan dihapus segera setelah proses selesai.

## Migrasi Media Antar Channel/Grup

- Dari menu utama bot, klik `Migrasi Media`.
- Pilih sumber (grup/channel/`Saved Messages`) lalu pilih tujuan.
- Masukkan limit jumlah pesan yang ingin disalin (hanya pesan yang memiliki media akan diproses). Ketik `semua` untuk tanpa batas.
- Bot akan memproses dari pesan paling lama ke terbaru, mencoba forward langsung dulu, dan jika forward dibatasi serta opsi re-upload aktif di Pengaturan, bot akan mengunduh dan mengunggah ulang media ke tujuan.
- Progres akan dikirimkan berkala di chat.

Catatan:
- Perintah `/migrasi` masih didukung sebagai alias, tetapi pengguna normal tidak perlu memakainya lagi.

- Migrasi memakai filter media pengguna jika telah disetel di menu Pengaturan (mis. hanya foto/video/dokumen).
- Deteksi duplikasi tetap aktif untuk menghindari unggahan ganda.
