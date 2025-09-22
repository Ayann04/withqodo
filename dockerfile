# Use slim Python image
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies & chromium
RUN apt-get update && apt-get install -y \
    wget curl unzip gnupg ca-certificates \
    chromium chromium-driver \
    fonts-liberation libasound2 libatk1.0-0 libcups2 \
    libdbus-1-3 libgdk-pixbuf2.0-0 libnspr4 libnss3 \
    libx11-xcb1 libxcomposite1 libxdamage1 libxrandr2 \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

# Set chrome & chromedriver paths
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

# Install Python dependencies
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project
COPY . .

# Run collectstatic (if needed)
RUN python manage.py collectstatic --noinput

# Expose Django default port
EXPOSE 8000

# Run server with gunicorn
CMD ["gunicorn", "scrapping.wsgi:application", "--bind", "0.0.0.0:8000", "--workers=2", "--threads=2"]
