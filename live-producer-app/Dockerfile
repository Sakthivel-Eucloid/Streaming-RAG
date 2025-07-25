# This Dockerfile sets up the Python environment for the live data producer.

# Use a slim Python base image.
FROM python:3.10-slim

# Set the working directory.
WORKDIR /app

# Copy and install dependencies.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application script.
COPY . .
