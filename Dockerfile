# Use the specified Python version
ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}

# Set the working directory
WORKDIR /app

# Copy requirements file and install dependencies
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt && \
    rm requirements.txt

# Install SpaCy
RUN pip install spacy

# Download SpaCy model
RUN python -m spacy download en_core_web_sm

# Copy the rest of the application code
COPY . .
