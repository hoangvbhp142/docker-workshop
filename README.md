
# docker-workshop

>Dự án học tập về pipeline xử lý dữ liệu sử dụng Docker, PostgreSQL, Pandas, SQLAlchemy và các công cụ hiện đại. Mục tiêu là dựng lại môi trường, ingest dữ liệu, và phát triển các pipeline ETL.

## Cấu trúc thư mục

```
docker-workshop/
│   README.md              # Hướng dẫn tổng quan
│
├── pipeline/              # Thư mục chính chứa pipeline và cấu hình
│   ├── config_data.py     # Cấu hình dataset ingest
│   ├── docker-compose.yaml# Cấu hình dịch vụ Docker
│   ├── Dockerfile         # Dockerfile cho ingest_data
│   ├── ingest_data.py     # Script ingest dữ liệu vào PostgreSQL
│   ├── main.py            # Script Python mẫu
│   ├── pipeline.py        # Script pipeline mẫu (Pandas)
│   ├── pyproject.toml     # Quản lý dependencies Python
│   ├── notebook.ipynb     # Notebook mẫu
│   └── README.md          # (Để trống)
│
├── test/                  # Thư mục test đơn giản
│   ├── file1.txt
│   ├── file2.txt
│   ├── file3.txt
│   └── script.py
```

## Yêu cầu hệ thống

- Docker & Docker Compose
- Python >= 3.12 (nếu chạy local ngoài Docker)

## Hướng dẫn cài đặt & sử dụng

### 1. Chạy toàn bộ pipeline với Docker Compose

```bash
cd pipeline
docker-compose up --build
```
*Các service sẽ được dựng:*
- **pgdatabase**: PostgreSQL 18, user: root, password: root, db: ny_taxi
- **pgadmin**: Truy cập tại http://localhost:8085 (user: admin@admin.com, pass: root)
- **ingest_data**: Container ingest dữ liệu vào PostgreSQL

### 2. Chạy ingest_data.py thủ công (nếu muốn)

```bash
cd pipeline
uv run python ingest_data.py --year 2021 --month 1
# hoặc dùng python trực tiếp nếu đã cài dependencies
python ingest_data.py --year 2021 --month 1
```
*Tham số có thể tuỳ chỉnh: --year, --month, --pg_user, --pg_password, --pg_db, --pg_host, --pg_port*

### 3. Cài đặt dependencies Python (nếu không dùng Docker)

```bash
cd pipeline
pip install -r requirements.txt  # hoặc dùng pyproject.toml với poetry/uv
```

### 4. Sử dụng notebook

Mở file `pipeline/notebook.ipynb` với JupyterLab hoặc VSCode để thử nghiệm các bước ingest, phân tích dữ liệu.

### 5. Test script đơn giản

```bash
cd test
python script.py
# In ra nội dung file1.txt
```

## Thông tin thêm về các file chính

- **ingest_data.py**: Script ingest dữ liệu NYC taxi (csv) vào PostgreSQL, dùng Pandas, SQLAlchemy, tqdm. Cấu hình dataset trong `config_data.py`.
- **docker-compose.yaml**: Dựng các service PostgreSQL, pgAdmin, ingest_data.
- **Dockerfile**: Build image Python, cài dependencies bằng uv, chạy ingest_data.py.
- **pyproject.toml**: Quản lý dependencies Python.
- **notebook.ipynb**: Notebook mẫu thao tác với dữ liệu.
- **pipeline.py, main.py**: Script mẫu thao tác với Pandas.

## Ghi chú

- Để tuỳ chỉnh ingest dataset, sửa file `config_data.py`.
- Để truy cập pgAdmin: http://localhost:8085 (user: admin@admin.com, pass: root)
- Dữ liệu sẽ được ingest vào các bảng trong PostgreSQL (xem logs ingest_data.py).

---

sudo pkill dockerd
sudo pkill containerd
sudo dockerd > /tmp/dockerd.log 2>&1 &