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
COPY ./docker_entrypoint.py ./docker_entrypoint.py
COPY ./main.py ./main.py
COPY ./processor ./processor
# Remove any .env file to ensure it's not included in the image
RUN rm -f .env

# Document required environment variables
ENV DATABASE_CONNECTION_URL=""

# Set entrypoint
ENTRYPOINT ["python", "docker_entrypoint.py", "--yesterday"]