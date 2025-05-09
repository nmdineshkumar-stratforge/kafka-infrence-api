FROM python:3.11-slim
WORKDIR /app

COPY . .

COPY ./worker/requirements.txt /app/requirements.txt
COPY backgroudprocess.py /app/

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "worker.py"]
