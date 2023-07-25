# Building Flights Data Pipeline using Apache Airflow

## [Project Overview]
Project ini merupakan tugas akhir saya sebagai Data Engineer Trainee pada Celerates Acceleration Program (CAP) 2023. Melalui project ini, saya akan melakukan ekstraksi data (sesuai _case_ dan _business process_ yang dipilih) yang berasal dari berbagai sumber, kemudian menerapkan beberapa teknik transformasi data, hingga menyimpannya ke dalam database terstruktur (Data Warehouse) agar dapat digunakan untuk kebutuhan analisa lebih lanjut. Kemudian, keseluruhan proses dari awal-akhir tentu akan di-_schedule_ agar dapat dijalankan secara otomatis sesuai rentang waktu yang diinginkan.

## [Requirements]
Project ini dijalankan pada _environment_ Linux OS (Ubuntu 20.04), dengan [MobaXterm](https://mobaxterm.mobatek.net/) sebagai tools yang memungkinkan pengguna me-render tampilan aplikasi yang berjalan di Linux melalui perangkat Windows, serta [Visual Studio Code](https://code.visualstudio.com/) sebagai code editor. Berikut adalah _requirement_ lain yang digunakan untuk menjalankan project ini.

- [Python](https://www.python.org/) versi 3.7.17
- [PySpark](https://spark.apache.org/docs/latest/api/python/#) versi 2.4.6
- [Apache Hadoop](https://hadoop.apache.org/) versi 2.8.5
- [Apache Hive](https://hive.apache.org/) versi 2.3.5
- [Apache Airflow](https://airflow.apache.org/) versi 2.6.0

Dalam kasus saya, setelah instalasi Airflow selesai dilakukan, pelu adanya pengaturan terkait _connection_ antara Airflow dengan Hive sebagai database yang akan digunakan. Pengaturan ini dapat dilakukan langsung melalui Web UI Airflow (Admin &rarr; Connection).

## [Data Sources]
Saya memilih data penerbangan yang disediakan oleh [OpenSky Network](https://opensky-network.org/) sebagai sumber data pada project ini. Melalui [OpenSky Network](https://opensky-network.org/), saya dapat mengakses data penerbangan (berdasarkan lokasi dan waktu yang dapat dikostumisasi), data pesawat, data bandara, hingga data manufaktur dari setiap penerbangan yang terdaftar pada situs. Lebih lanjut, saya memilih untuk memfokuskan melakukan penarikan data penerbangan pada bandara **Hartsfield-Jackson** sebagai salah satu bandara tersibuk di dunia. Berikut adalah beberapa data yang saya gunakan pada project ini.

- Flight Data (REST API)
- aircraftDatabase (CSV file)
- aircraftTypes (CSV file)
- Airport (Python API)

Catatan: Data berformat CSV dapat diakses [disini](https://opensky-network.org/datasets/metadata/), sedangkan dokumentasi penggunaan REST API dan/atau Python API dapat diakses [disini](https://openskynetwork.github.io/opensky-api/rest.html).

## [Data Architecture]
Berikut adalah data arsitektur yang diimplementasikan dalam project ini.

<img src='https://drive.google.com/uc?export=view&id=1zt_ll4F5G_4A2cKPp06cnIiyc6z-d7cw'>

\
Dalam keberjalanannya, proses ini menjalankan 2 DAG berbeda yang keduanya memiliki _objective_ masing-masing. Berikut adalah deskripsi secara lebih spesifik.

- `fp_1month_dag.py` merupakan DAG yang dijalankan setiap sebulan sekali. DAG ini akan memproses data `aircraftDatabase`, `aircraftTypes`, dan `Airport` yang nantinya setelah melalui proses transformasi akan menghasilkan tabel `dim_aircraft` dan `dim_airports`. Berikut adalah gambar _flow_ DAG Monthly saat berhasil dijalankan pada Apache Airflow.
  <img src='https://drive.google.com/uc?export=view&id=1veahgcwAYfRSF_PUDMwR2AZHlUZP9t_m'>
- `fp_daily_dag.py` merupakan DAG yang dijalankan setiap hari. DAG ini akan memproses data `Flight` yang diperoleh dari REST API yang nantinya setelah melalui proses transformasi akan menghasilkan tabel `fact_flight`. Berikut adalah gambar _flow_ DAG Daily saat berhasil dijalankan pada Apache Airflow.
  <img src='https://drive.google.com/uc?export=view&id=1ABfy2mwj_dWiVHx65P1AkherJnmOU1qm'>

Catatan: Pada Data Warehouse akan terdapat `dim_date` dan `dim_time` yang mana di-_generate_ secara manual.


## [Database Schema]
Berikut adalah data model dari Data Warehouse yang dibuat.


<img src='https://drive.google.com/uc?export=view&id=1wbFXjRM6Ep2atoYWjyVHJCy1hXPDkflO'>

\
Berikut adalah data dictionary untuk tabel `fact_flight`.
|     Nama Kolom    | Tipe Data |                   Deskripsi                   |  Contoh  |
|:-----------------:|:---------:|:---------------------------------------------:|:--------:|
| icao24            | STRING    | Kode pesawat dengan format ICAO               | a6f5d7   |
| sk_departure_date | BIGINT    | SK tanggal keberangkatan pesawat              | 20230720 |
| sk_departure_time | BIGINT    | SK jam keberangkatan pesawat                  | 18       |
| departure_airport | STRING    | Kode bandara keberangkatan dengan format ICAO | KATL     |
| sk_arrival_date   | BIGINT    | SK tanggal sampai pesawat                     | 20230720 |
| sk_arrival_time   | BIGINT    | SK jam sampai pesawat                         | 19       |
| arrival_airport   | STRING    | Kode bandara tujuan dengan format ICAO        | KMGM     |
| callsign          | STRING    | Nomor penerbangan                             | SKW3949  |
| flight_duration   | BIGINT    | Durasi penerbangan (detik)                    | 6080     |

\
Berikut adalah data dictionary untuk tabel `dim_aircraft`.
|     Nama Kolom     | Tipe Data |             Deskripsi             |          Contoh         |
|:------------------:|:---------:|:---------------------------------:|:-----------------------:|
| icao24             | STRING    | Kode pesawat dengan format ICAO   | ae6963                  |
| manufacturer_code  | STRING    | Kode pembuat pesawat              | WREN                    |
| registration_code  | STRING    | Kode registrasi pembuatan pesawat | N1936X                  |
| operator           | STRING    | Operator pesawat                  | United States Air Force |
| owner              | STRING    | Pemilik pesawat                   | Nelson William T        |
| model              | STRING    | Model pesawat                     | C-130J Hercules C.5     |
| serial_number      | STRING    | Serial Number pesawat             | 1LK531C                 |
| aircraft_desc      | STRING    | Deskripsi pesawat                 | LandPlane               |
| engine             | STRING    | Nama mesin pesawat                | CONT MOTOR TSIO-520-E   |
| engine_type        | STRING    | Tipe mesin pesawat                | Piston                  |
| engine_count       | INTEGER   | Banyak mesin pada pesawat         | 1                       |
| built_date         | DATE      | Tanggal pembuatan pesawat         | 1968-01-01              |
| registration_start | DATE      | Tanggal mulai registrasi pesawat  | 2012-07-13              |
| registration_end   | DATE      | Tanggal akhir registrasi pesawat  | 2028-09-30              |
| data_updated_at    | DATE      | Timestamp saat data diperbarui    | 2023-06-04              |

\
Berikut adalah data dictionary untuk tabel `dim_airports`.
|    Nama Kolom   | Tipe Data |                           Deskripsi                          |     Contoh     |
|:---------------:|:---------:|:------------------------------------------------------------:|:--------------:|
| icao            | STRING    | Kode bandara dengan format ICAO                              | US-0364, UENN  |
| iata            | STRING    | Kode bandara dengan format IATA                              | NYR            |
| name            | STRING    | Nama bandara                                                 | Malina Airport |
| country         | STRING    | Negara asal bandara                                          | United States  |
| municipality    | STRING    | Kota asal bandara                                            | Marshalltown   |
| latitude        | DOUBLE    | Posisi bandara berdasarkan garis lintang                     | 42.000789      |
| longitude       | DOUBLE    | Posisi bandara berdasarkan garis bujur                       | 92.915239      |
| altitude        | DOUBLE    | Posisi ketinggian bandara dari permukaan laut (satuan: feet) | 994.0          |
| type            | STRING    | Tipe bandara.                                                | small_airport  |
| data_updated_at | DATE      | Timestamp saat data diperbarui                               | 2023-06-04     |

\
Berikut adalah data dictionary untuk tabel `dim_date`.
|      Nama Kolom     | Tipe Data |                Deskripsi                |     Contoh     |
|:-------------------:|:---------:|:---------------------------------------:|:--------------:|
| sk_date             | BIGINT    | SK dimensi tanggal                      | 20200101       |
| date_th             | STRING    | Tanggal                                 | 2020-01-01     |
| year_th             | BIGINT    | Tahun                                   | 2020           |
| month_th            | BIGINT    | Bulan                                   | 1              |
| day_th              | BIGINT    | Hari                                    | 1              |
| quarter_th          | BIGINT    | Quarter                                 | 1              |
| week_number         | BIGINT    | Minggu ke-i dalam tahun                 | 1              |
| is_weekend          | BOOLEAN   | Masuk atau tidaknya pada hari Weekend   | False          |
| is_public_holiday   | BOOLEAN   | Masuk atau tidaknya pada libur nasional | True           |
| holiday_description | STRING    | Keterangan libur nasional               | New Year's Day |

\
Berikut adalah data dictionary untuk tabel `dim_time`.
|  Nama Kolom | Tipe Data |                Deskripsi               | Contoh |
|:-----------:|:---------:|:--------------------------------------:|:------:|
| hour        | BIGINT    | Jam                                    | 17     |
| am_pm       | STRING    | Keterangan masuk waktu AM/PM           | PM     |
| is_rushhour | STRING    | Masuk atau tidaknya ke waktu Rush Hour | Yes    |
