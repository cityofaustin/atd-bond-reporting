FROM python:3.8-slim

# Copy our own application
WORKDIR /app
COPY . /app/atd-bond-reporting

RUN chmod -R 755 /app/*

# # Proceed to install the requirements...do
RUN cd /app/atd-bond-reporting && apt-get update && \
    pip install -r requirements.txt