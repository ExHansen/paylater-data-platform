# paylater-data-platform — Desain

- **Tanggal:** 2026-11-06
- **Status:** Menunggu review (desain saja, belum ada implementasi)
- **Tujuan:** Proyek portofolio kedua untuk CV Data/AI Engineer. Fokusnya keterampilan yang paling sering diminta lowongan Data Engineer di Indonesia dan belum tercakup proyek pertama (`llm-doc-extractor`).

---

## 1. Latar belakang

### 1.1 Riset lowongan (LinkedIn, Indonesia, November 2026)

Ada 783 lowongan Data Engineer dan 608 lowongan AI Engineer. Isi 21 lowongan Data Engineer dibaca, antara lain dari Sea/Monee, Grab, Kredivo/FinAccel, BukuWarung, Bibit, OCBC, Astra, Deloitte, dan EY.

**Kemunculan kata kunci** (dari 21 lowongan):

| Kata kunci | Jumlah |
|---|---|
| SQL | 17 |
| ETL/ELT | 17 |
| Python | 14 |
| Data modeling | 14 |
| Performance tuning | 14 |
| Data quality | 13 |
| Spark | 12 |
| Data warehouse | 11 |
| Airflow | 10 |
| Streaming/real-time | 10 |
| Keamanan/PII/compliance | 9 |
| Monitoring/alerting | 8 |
| Governance/lineage | 8 |
| Kafka | 7 |
| dbt | 5 |

**Masalah yang tertulis di deskripsi kerja:**
- **Fintech kredit** (Sea/Monee, Kredivo, BukuWarung):
  - pipeline batch + real-time untuk fitur risiko kredit;
  - data mart bisnis;
  - otomasi laporan regulasi;
  - "ensure data quality, accuracy, and pipeline reliability".
- **E-commerce/beauty** (Facetology, Scarlett): monitoring, alerting, dan pengecekan kualitas otomatis pada pipeline.

**Survei industri** (ringkasan pihak kedua; sebagian bersumber dari vendor):
- Sekitar 34% waktu tim data habis untuk kualitas dan reliabilitas data, dan sekitar setengahnya untuk merawat pipeline yang ada.
- *Schema drift* adalah penyebab besar beban kerja.
- Banyak organisasi butuh lebih dari 4 jam untuk *mendeteksi* insiden data quality.

### 1.2 Yang disampaikan pengguna
- Membangun proyek Data/AI Engineer berdasarkan kebutuhan lowongan; memilih **platform data fintech**.
- Memakai Docker, dengan **pendekatan A** (stack modern ukuran laptop).
- Folder: `C:\Users\User\Work\paylater-data-platform`.
- **Laptop tidak punya resource untuk menjalankan stack.** Tahap ini hanya desain. Eksekusi berat dilakukan di GitHub Codespaces atau GitHub Actions, bukan di laptop.

### 1.3 Asumsi
- Laptop: Windows 11 Enterprise LTSC, Ryzen 5 3500U, RAM 22 GB, sisa disk sekitar 15 GB, Python 3.11 dan uv tersedia. Cukup untuk menulis kode dan unit test.
- Repo nantinya publik di akun GitHub ExHansen, sama seperti proyek pertama.
- README berbahasa Inggris. Dokumen desain berbahasa Indonesia.

---

## 2. Kriteria sukses

1. `pdp up --profile core`, lalu `pdp seed`, lalu `pdp live`: event CDC masuk ke `raw` dalam hitungan detik, dan alert risiko keluar dengan **p95 latensi commit → alert < 5 detik**, terukur di CI.
2. `dbt build` lulus di CI terhadap fixture CDC, termasuk duplikat, event tak berurutan, dan delete. Nilai mart (termasuk TWP90) sama dengan angka yang diharapkan.
3. Di tes end-to-end, **setiap jenis fault yang disuntikkan terdeteksi**, dan MTTD per fault dilaporkan. Target: ≤ 10 menit untuk cek 5-menitan.
4. Laporan regulasi bulanan (Excel) terbentuk, dan totalnya cocok dengan mart.
5. CI hijau: lint + unit + dbt + e2e. Ringkasan e2e terbit di halaman CI (*job summary*) dan sebagai artifact.
6. Codespaces terbuka dari repo dan bisa menjalankan profil `full`.
7. README (Inggris) memuat diagram arsitektur, quickstart Codespaces, hasil dari CI, keputusan desain, kamus data, dan keterbatasan.

---

## 3. Arsitektur

```
 Simulator ──► Postgres "paylater_app" (OLTP, wal_level=logical)
                    │  Debezium (pgoutput, publication pdp_pub)
                    ▼
               Kafka (KRaft, 1 broker) — topik paylater.public.<tabel>, JSON
          ┌─────────┴──────────┐
          ▼                    ▼
   Stream processor       Raw loader ──► Postgres "warehouse".raw (bronze)
   (Quix Streams)                          │ dbt: staging → intermediate → marts
   ├► online.risk_features                 ▼
   └► topik risk.alerts ──► online.risk_alerts
                                     Airflow: pdp_quality_5min · pdp_hourly ·
                                              pdp_daily_dq · pdp_monthly_regulatory
                                           │
                                     ops.dq_results · ops.alerts · ops.fault_log
                                           │
                                     Dashboard Streamlit
```

Satu instance Postgres memuat dua database:
- `paylater_app`: OLTP, sumber CDC.
- `warehouse`: berisi schema `raw`, `staging`, `intermediate`, `marts`, `online`, `ops`, dan metadata Airflow (`airflow`).

---

## 4. Komponen

### 4.1 Data model OLTP (`paylater_app.public`)

| Tabel | Kolom utama | Catatan |
|---|---|---|
| `users` | user_id (uuid), full_name, nik (16 digit, palsu), phone, province, city, kyc_status (`pending/verified/rejected`), created_at, updated_at | NIK/phone = PII |
| `credit_accounts` | account_id, user_id, credit_limit, status (`active/frozen/closed`), risk_grade (`A`–`E`), created_at, updated_at | perubahan limit menghasilkan riwayat SCD2 |
| `merchants` | merchant_id, name, category, province, created_at | |
| `transactions` | txn_id, account_id, merchant_id, amount, tenor_months, monthly_rate, status (`approved/declined/refunded`), channel (`app/web/qr`), device_id, created_at, updated_at | **sengaja tanpa CHECK** pada amount/tenor, supaya "bug aplikasi" bisa lolos ke data |
| `installments` | installment_id, txn_id, account_id, seq, due_date, principal, interest, status (`unpaid/paid`), paid_at, updated_at | dibuat saat transaksi disetujui; `account_id` didenormalisasi supaya stream bisa memperbarui exposure akun |
| `repayments` | repayment_id, installment_id, account_id, amount, method (`va/ewallet/retail`), paid_at | |

`REPLICA IDENTITY FULL` diaktifkan pada semua tabel, supaya event update dan delete membawa nilai `before`.

### 4.2 Simulator (`src/pdp/simulator`)

**Mode seed** (`pdp seed --months 12 --seed 42`):
- Isi awal: sekitar 5.000 user (sebaran provinsi berbobot populasi), 300 merchant, dan sekitar 60.000 transaksi selama 12 bulan.
- Tenor: 1/3/6/12 bulan, dengan bobot 40/30/20/10.
- Bunga per bulan menurut risk grade: A 2,0%, B 2,5%, C 3,0%, D 3,5%, E 4,0%.
- Perilaku bayar menurut grade: tepat waktu, telat 1–90 hari, atau macet (> 90 hari). Probabilitas macet: A 1%, B 2%, C 4%, D 8%, E 15%.
- Ditulis dengan `COPY` massal. Debezium mengambilnya lewat *initial snapshot*.

**Mode live** (`pdp live --rate 5`):
- Transaksi baru dengan rata-rata 5 per detik (Poisson).
- Pembayaran untuk cicilan yang jatuh tempo.
- Kadang perubahan limit atau status akun.
- Sebagian kecil akun "nakal" memicu aturan risiko: rentetan transaksi, device baru, atau nominal mendekati limit.

**Deterministik:** `random.Random(seed)`. Waktu memakai jam nyata. Histori seed berakhir pada "sekarang".

### 4.3 Fault injection (`pdp fault <jenis>`)

Setiap injeksi menulis satu baris ke `ops.fault_log(fault_id, fault_type, injected_at, details)`.

| Jenis | Cara | Dideteksi oleh |
|---|---|---|
| `duplicate_events` | Produce ulang 50 pesan CDC lama ke topik | cek `raw_duplicate_lsn` (5 menit) |
| `out_of_order` | Produce versi lama dari baris yang sudah diperbarui | cek `lsn_regression` (5 menit); staging tetap memilih `lsn` terbesar |
| `schema_drift` | `ALTER TABLE transactions ADD COLUMN promo_code text` lalu mengisinya | cek `schema_drift` terhadap kontrak `infra/contracts/*.yml` (5 menit) |
| `invalid_amount` | Insert transaksi dengan amount 0 atau negatif | cek `invalid_values` (5 menit) + tes dbt |
| `invalid_tenor` | Insert transaksi dengan tenor 24 | cek `invalid_values` (5 menit) + tes dbt `accepted_values` |
| `loader_stall` | Hentikan loader N menit | cek `freshness` (5 menit, ambang 10 menit) |
| `volume_drop` | Rate transaksi 0 selama 1 jam | cek `volume_anomaly` (harian; e2e memicunya manual) |
| `reconciliation_gap` | Loader membuang satu batch (flag bug) | cek `reconciliation` (harian; e2e memicunya manual) |

### 4.4 Stream processor (`src/pdp/stream`, Quix Streams)

- **Input:** topik `paylater.public.transactions` dan `paylater.public.credit_accounts` (untuk limit). Hanya event `c` (create) dengan status `approved` yang dihitung.
- **State per `account_id`:**
  - window bergeser 10 menit dan 1 jam (jumlah, nominal);
  - outstanding (pokok transaksi aktif dikurangi pembayaran);
  - limit terkini;
  - himpunan `device_id` 30 hari terakhir.
- **Fitur:** `txn_count_10m`, `txn_amount_10m`, `txn_count_1h`, `txn_amount_1h`, `utilization`, `is_new_device`. Ditulis (upsert) ke `online.risk_features`.

**Aturan alert** (fungsi murni di `rules.py`):

| ID | Kondisi |
|---|---|
| `RR1_velocity` | `txn_count_10m ≥ 5` |
| `RR2_near_limit` | `amount > 0.8 × credit_limit` |
| `RR3_new_device_high` | `is_new_device` dan `amount > 2.000.000` |
| `RR4_utilization` | `utilization > 0.95` |

- **Exposure dan limit lintas topik:** disimpan di tabel `online.account_exposure` (bertambah saat transaksi disetujui, berkurang saat cicilan lunas) dan `online.account_limits` (dari topik `credit_accounts`). Konsistensinya *eventual*; utilisasi bisa tertinggal sesaat.
- **Output alert:** topik `risk.alerts` dan tabel `online.risk_alerts`. Isinya `latency_ms` = waktu alert − `source.ts_ms` (waktu commit di Postgres).

### 4.5 Raw loader (`src/pdp/loader`)

- Mengonsumsi semua topik `paylater.public.*`, group `pdp-raw-loader`.
- Batch per 500 pesan atau 5 detik.
- **Tabel tujuan** `raw.cdc_<tabel>`:

| Kolom | Isi |
|---|---|
| `op` | jenis operasi CDC |
| `pk` | primary key |
| `before`, `after` | jsonb |
| `source_ts_ms` | waktu commit di sumber |
| `lsn` | log sequence number |
| `kafka_topic`, `kafka_partition`, `kafka_offset` | posisi pesan di Kafka |
| `ingested_at` | waktu masuk warehouse |

- **Unik** pada (topic, partition, offset), sehingga restart tidak menggandakan data.
- **Offset di-commit setelah transaksi database berhasil** (*at-least-once* + idempotent = *effectively once*).
- Event *tombstone* diabaikan. Field yang tidak dikenal tetap disimpan di jsonb, sehingga schema drift tidak menghentikan loader dan hanya terdeteksi oleh cek.

### 4.6 Warehouse dbt (`dbt/`, adapter dbt-postgres)

**staging** (`stg_<tabel>`):
- Mengambil baris terbaru per PK menurut `lsn`, lalu `source_ts_ms`.
- Membuang delete dan meng-cast tipe data.
- **PII:** `nik` dan `phone` diganti `sha256(salt || nilai)`, dan `full_name` dibuang. Kolom ditandai `meta: {pii: true}`.

**intermediate:**
- `int_installment_dpd`: DPD per cicilan per tanggal.
  - `DPD = as_of_date − due_date` jika belum lunas dan sudah lewat jatuh tempo; selain itu 0.
- `int_credit_limit_history`: SCD2 dari pasangan `before`/`after` CDC, dengan kolom `valid_from`/`valid_to`.

**marts:**
- **Dimensi dan fakta:** `dim_user`, `dim_merchant`, `dim_date`, `fct_transactions`, `fct_installments`, `fct_repayments`.
- `mart_portfolio_daily`: per tanggal, berisi outstanding pokok, penyaluran, peminjam aktif, dan jumlah transaksi.
- `mart_delinquency`: per tanggal dan provinsi.
  - Bucket DPD: lancar, 1–30, 31–60, 61–90, > 90.
  - **TWP90** = Σ outstanding pokok transaksi dengan DPD maksimum cicilan > 90 ÷ Σ outstanding pokok. Istilah ini mengikuti definisi OJK untuk *tingkat wanprestasi 90 hari*.
- `mart_collections`: tingkat pembayaran per metode dan per bucket DPD.
- `mart_merchant_performance`: GMV, jumlah transaksi, dan rasio wanprestasi per merchant.

**Tes:**
- Tes standar: `unique`, `not_null`, `relationships`, `accepted_values` (tenor ∈ {1,3,6,12}, status).
- Tes bisnis: `amount > 0`; Σ pokok cicilan = amount transaksi (toleransi Rp 1); pembayaran ≤ tagihan.
- **Model contracts** (`contract: enforced`) pada semua mart.

### 4.7 Data quality dan observability (`src/pdp/quality`)

**Daftar cek** (setiap cek menulis ke `ops.dq_results`: check_id, check_type, target, status `pass/warn/fail`, observed, expected, run_at):

| Cek | Cara | Status |
|---|---|---|
| `freshness` | `now − max(ingested_at)` per tabel raw dibandingkan ambang | fail > 10 menit saat live |
| `raw_duplicate_lsn` | jumlah `lsn` sama dengan offset berbeda dalam 15 menit terakhir | warn > 0 |
| `lsn_regression` | event yang `lsn`-nya lebih kecil dari versi yang sudah ada untuk PK sama | warn > 0 |
| `schema_drift` | himpunan field `after` pada event terbaru vs kontrak YAML | fail jika ada field baru atau hilang |
| `invalid_values` | amount ≤ 0, atau tenor ∉ {1,3,6,12}, pada raw 15 menit terakhir | fail > 0 |
| `volume_anomaly` | z-score jumlah transaksi pada jendela terakhir vs N jendela sebelumnya (default jendela 60 menit, N = 24; e2e memakai 1 menit, N = 10). Baseline bergulir dipakai karena laju live jauh lebih tinggi daripada histori seed | fail jika \|z\| > 3 |
| `reconciliation` | count dan Σ amount per tabel di OLTP vs staging, dengan as-of yang sama | fail jika selisih > 0,1% |

- **Alert:** setiap `fail` menulis ke `ops.alerts`. Opsional, alert dikirim ke webhook Slack/Telegram lewat `PDP_ALERT_WEBHOOK`.
- **MTTD** per fault = `run_at` cek gagal pertama yang dipetakan ke jenis fault itu − `injected_at` di `ops.fault_log`. Hasilnya disimpan di view `ops.v_mttd`.

### 4.8 Airflow (`airflow/dags`, Airflow 3.3 standalone, LocalExecutor)

| DAG | Jadwal | Isi |
|---|---|---|
| `pdp_quality_5min` | tiap 5 menit | freshness, duplicate, lsn_regression, schema_drift, invalid_values |
| `pdp_hourly` | tiap jam | cek freshness → `dbt build` (staging+, run + test) → simpan hasil tes dbt ke `ops.dq_results` |
| `pdp_daily_dq` | harian 01:00 WIB | reconciliation, volume_anomaly, ringkasan harian data quality |
| `pdp_monthly_regulatory` | tanggal 1, 02:00 WIB | laporan regulasi bulan sebelumnya |

Semua DAG memanggil fungsi di `src/pdp`, sehingga logikanya bisa dites tanpa Airflow. Catatan: desain awal menyebut 3 DAG; cek 5-menitan dipisah ke DAG sendiri supaya MTTD bisa ≤ 10 menit.

### 4.9 Laporan regulasi (`src/pdp/reports/regulatory.py`)

- Excel `reports/regulatory_<YYYY-MM>.xlsx` dengan sheet: ringkasan (outstanding akhir bulan, penyaluran, peminjam aktif, TWP90), per provinsi, dan bucket DPD.
- Pengecekan otomatis: total sheet provinsi = ringkasan = `mart_portfolio_daily` pada akhir bulan.
- **Label jujur:** "terinspirasi dari indikator OJK, bukan format resmi".

### 4.10 Dashboard (`src/pdp/dashboard`, Streamlit)

1. **Kesehatan pipeline:** freshness per tabel, lag konsumer, tingkat lulus data quality 24 jam, alert terbuka, dan tabel MTTD.
2. **Alert risiko:** feed terbaru dan latensi p50/p95.
3. **KPI portofolio:** outstanding, tren TWP90, peta/tabel per provinsi, dan merchant teratas.

Kolom PII tidak pernah ditampilkan.

---

## 5. Menjalankan

### 5.1 Docker Compose (versi image dipin di rencana implementasi)

| Service | Profil | Batas memori |
|---|---|---|
| postgres (16, `wal_level=logical`) | core | 768 MB |
| kafka (KRaft, 1 broker) | core | 1 GB |
| connect (Debezium) | core | 1 GB |
| simulator, loader, stream (satu image app, beda command) | core | 256 MB masing-masing |
| airflow (standalone) | full | 2 GB |
| dashboard | full | 384 MB |

- Total: `core` sekitar 3,3 GB, `full` sekitar 5,7 GB.
- **Kesiapan service:**
  - setiap service punya healthcheck;
  - service `connect-init` mendaftarkan konektor Debezium dari `infra/connect/postgres-cdc.json`;
  - `postgres` menjalankan `infra/postgres/init.sql` untuk membuat database, schema, publication, dan role.

### 5.2 Tempat menjalankan
- **GitHub Codespaces:** `.devcontainer/devcontainer.json` (Python 3.11, uv, docker-in-docker, mesin minimal 4-core/16 GB; port 8080 Airflow dan 8501 dashboard di-forward). Akun gratis punya kuota bulanan terbatas.
- **GitHub Actions:** e2e berjalan di `ubuntu-latest` (repo publik).
- **Laptop pengguna:** hanya untuk menulis kode, unit test, dan `dbt parse`. Tidak perlu Docker.

### 5.3 CLI `pdp` (Typer)

| Perintah | Fungsi |
|---|---|
| `up --profile core\|full` | menyalakan stack |
| `down [--volumes]` | mematikan stack |
| `seed --months 12 --seed 42` | mengisi histori |
| `live --rate 5 --duration 10m` | menjalankan transaksi live |
| `fault <jenis>` | menyuntikkan fault |
| `quality run [--checks ...]` | menjalankan cek data quality |
| `dbt-build` | menjalankan `dbt build` |
| `report --month 2026-10` | membuat laporan regulasi |
| `status` | ringkasan service, lag, freshness, alert |
| `e2e` | skenario end-to-end penuh + `e2e-summary.md` |

### 5.4 Konfigurasi
`pydantic-settings` dengan prefiks `PDP_`: DSN Postgres, bootstrap Kafka, salt PII, webhook alert, dan ambang cek. Nilai default cocok untuk Compose.

---

## 6. Penanganan error
- **Loader:**
  - pesan rusak atau tidak bisa di-parse dikirim ke `raw.cdc_dead_letter` (payload, error, offset) tanpa menghentikan konsumsi;
  - kalau database mati, loader retry dengan backoff dan offset tidak di-commit.
- **Stream:** event tanpa field wajib dilewati dan dihitung di metrik `stream_skipped_total`. Satu event buruk tidak menghentikan aplikasi.
- **Cek data quality:** setiap cek berdiri sendiri. Exception pada satu cek menjadi status `error` di `ops.dq_results` tanpa menggagalkan cek lain.
- **Airflow:** retry 2× dengan jeda 1 menit. Kegagalan task memicu callback yang menulis ke `ops.alerts`.
- **CLI:** `pdp up` menunggu healthcheck dengan timeout yang jelas, lalu mencetak service yang tidak sehat beserta saran `docker compose logs <service>`.

## 7. Testing

**Unit (pytest, tanpa Docker):**
- determinisme simulator, distribusi tenor/grade, cicilan = pokok;
- fitur window dan aturan RR1–RR4;
- parsing envelope Debezium (c/u/d/r, tombstone, field baru);
- penulisan idempotent (lewat Postgres tiruan atau fungsi pembentuk SQL);
- z-score, selisih rekonsiliasi, diff skema vs kontrak;
- hashing PII;
- pembentukan dan validasi laporan Excel;
- pemetaan fault → cek → MTTD.

**dbt (CI):**
- service container Postgres 16 dengan fixture `raw.cdc_*` (CSV/SQL) yang memuat duplikat, event tak berurutan, delete, dan histori limit;
- `dbt build` harus lulus;
- tes SQL tambahan mencocokkan nilai mart dengan angka yang diharapkan: TWP90, bucket DPD, dan baris SCD2.

**End-to-end (`pdp e2e`, CI dan Codespaces):**
1. `up full` → `seed --months 3` (dipersingkat untuk CI) → `live` 5 menit.
2. Suntikkan semua jenis fault berurutan. Cek harian dipicu manual.
3. Assert:
   - alert RR1–RR4 muncul, dengan p95 latensi < 5 detik;
   - mart terisi dan TWP90 terhitung;
   - setiap fault terdeteksi;
   - laporan regulasi terbentuk.
4. Tulis `e2e-summary.md` (latensi, MTTD per fault, jumlah baris mart) ke job summary CI dan artifact.

**Lint:** `ruff`, `dbt parse`, validasi `docker compose config`.

## 8. Struktur repo
```
paylater-data-platform/
├─ docker-compose.yml  .env.example  .devcontainer/devcontainer.json
├─ .github/workflows/  ci.yml (lint + unit + dbt)  e2e.yml (push ke main + manual)
├─ docker/  app.Dockerfile  airflow.Dockerfile
├─ infra/  postgres/init.sql  connect/postgres-cdc.json  contracts/<tabel>.yml
├─ src/pdp/
│  ├─ config.py  db.py  cli.py
│  ├─ simulator/  domain.py  generator.py  seed.py  live.py  faults.py
│  ├─ stream/     features.py  rules.py  app.py
│  ├─ loader/     envelope.py  writer.py  app.py
│  ├─ quality/    checks.py  reconcile.py  drift.py  alerts.py  mttd.py
│  ├─ reports/    regulatory.py
│  └─ dashboard/  app.py
├─ dbt/  dbt_project.yml  profiles.yml  models/{staging,intermediate,marts}  macros/  tests/
├─ airflow/dags/  pdp_quality_5min.py  pdp_hourly.py  pdp_daily_dq.py  pdp_monthly_regulatory.py
├─ tests/  unit/  dbt/ (fixture + assert)  e2e/
├─ docs/  superpowers/specs/  superpowers/plans/  data-dictionary.md
└─ pyproject.toml  uv.lock  README.md  LICENSE (MIT)
```

## 9. Di luar cakupan (roadmap)
- Spark/Flink dan lakehouse (Iceberg/Delta + MinIO + Trino).
- Schema Registry + Avro (tahap ini pakai JSON), dan cloud warehouse (BigQuery/Snowflake).
- Model ML scoring risiko kredit. `online.risk_features` disiapkan sebagai *online feature store*-nya.
- Render model dbt per task di Airflow lewat Astronomer Cosmos, dan Terraform/IaC.
- Autentikasi dashboard dan multi-tenant.

## 10. Risiko
- **Image Docker besar** (sekitar 5 GB), sehingga CI e2e lambat (perkiraan 10–15 menit). Mitigasi: cache layer, seed dipersingkat, e2e hanya di `main` dan manual.
- **Kuota Codespaces gratis terbatas.** Mitigasi: e2e di CI sebagai bukti utama, dan Codespaces hanya untuk demo interaktif.
- **Debezium/Kafka sensitif terhadap urutan start.** Mitigasi: healthcheck + `connect-init` dengan retry.
