# This Dockerfile sets up the Python environment for the consumer service.

# Use a slim Python base image for a smaller footprint.
FROM python:3.10-slim

# Set the working directory inside the container.
WORKDIR /app

# Copy the requirements file first to leverage Docker's build cache.
COPY requirements.txt .

# Install the Python dependencies.
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application source code.
COPY . .

# Specify the command to run when the container starts.
CMD ["python", "main.py"]
