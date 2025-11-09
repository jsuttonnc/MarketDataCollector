# Use ARM-compatible base image
FROM python:3.12-slim

# Set environment variables to avoid buffering
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Copy only the requirements first to leverage Docker cache
COPY requirements.txt .

# Install dependencies using UV with full path
RUN pip install --no-cache-dir -r requirements.txt


# Copy the rest of your application
COPY . .

# Run the application
CMD ["python", "main.py"]