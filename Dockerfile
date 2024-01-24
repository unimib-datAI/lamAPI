ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt && \
    pip install -U pip setuptools wheel && \
    pip install -U spacy==3.7.2 && \
    python -m spacy download en_core_web_sm && \
    rm requirements.txt

COPY . .
