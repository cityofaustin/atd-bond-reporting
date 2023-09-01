FROM python:3.8-slim

# Install supporting OS support for building python libraries
RUN apt-get update
RUN apt-get install -y build-essential

# Copy in our application
WORKDIR /app
COPY . /app/atd-bond-reporting
RUN chmod -R 755 /app/*

# Install the requirements
RUN cd /app/atd-bond-reporting && pip install -r requirements.txt
