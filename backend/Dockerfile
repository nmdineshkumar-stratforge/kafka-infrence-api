FROM python:3.11-slim
WORKDIR /app

COPY . .

COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

CMD ["uvicorn", "Backend:app","--port", "8000"]
