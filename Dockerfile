# Use the specified Python version
ARG PYTHON_VERSION
FROM python:3.9

# Set the working directory
WORKDIR /app

# Copy requirements file and install dependencies
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt && \
    rm requirements.txt

# Copy the rest of the application code
COPY . .