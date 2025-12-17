# Image này đã có sẵn Spark và Java
FROM jupyter/all-spark-notebook:latest

USER root

# Cài thêm các thư viện cần thiết
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Thiết lập thư mục làm việc
WORKDIR /app

USER ${NB_UID}