FROM python:3.11-slim

RUN apt-get update && apt-get install -y ffmpeg && apt-get clean

WORKDIR /app

# Copy code into container
COPY . .

# Install requirements
COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "worker.py"]
