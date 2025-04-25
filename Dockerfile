FROM python:3.13.3-slim

# Install PostgreSQL client dependencies
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better layer caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy remaining files
COPY . .

# Set entrypoint
ENTRYPOINT ["python", "main.py"]