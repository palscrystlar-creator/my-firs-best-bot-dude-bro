# IELTS Speaking Bot uchun Docker image
FROM python:3.11-slim

# ffmpeg — pydub (gTTS mp3 -> ogg/opus konvertatsiyasi) uchun majburiy
# fonts-dejavu-core — sertifikat rasmidagi matnlarni chizish uchun (Pillow/PIL)
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg fonts-dejavu-core && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# Render "Web Service" portni kutadi — kod PORT env varni o'zi o'qiydi
ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
