FROM apache/airflow:2.7.3
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir easyocr
RUN pip3 install --no-cache-dir requests
RUN pip install --no-cache-dir apache-airflow-providers-postgres