FROM apache/airflow:3.3.2-python3.12

# torch solo CPU (sin dependencias CUDA) y Laya fijado a la versión validada en el spike
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu \
 && pip install --no-cache-dir \
      laya==0.3.20 rapidfuzz==3.* jellyfish==1.* phonenumbers==9.* unidecode==1.* \
      faker==37.* scikit-learn==1.* pandas==2.* pytest==8.*
