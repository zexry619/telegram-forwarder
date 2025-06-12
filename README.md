# Telegram Forwarder

Aplikasi ini meneruskan berbagai media dari semua chat (kecuali yang Anda kecualikan) ke satu chat tujuan.

## Fitur Utama
- Login dan manajemen sesi akun Telegram.
- Pilihan target chat dan daftar pengecualian.
- Filter tipe media yang diteruskan (foto, video, dokumen atau semua).
- Otomatis menghindari duplikasi pesan.
- Deteksi duplikat video menggunakan hash thumbnail tanpa mengunduh seluruh file.
- Penjadwalan otomatis untuk menyalakan atau menghentikan worker pada jam tertentu.
- Pesan yang tidak dapat diteruskan karena pembatasan (forward restricted) akan langsung dilewati tanpa proses unduh dan unggah ulang.

Semua pengaturan dapat dilakukan melalui menu bot setelah Anda menjalankan aplikasi.
