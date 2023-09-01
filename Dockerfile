FROM python:3.8-slim

# Install supporting OS support for building python libraries
RUN apt-get update
RUN apt-get install -y build-essential

# Copy in our application
COPY . /app/atd-bond-reporting

# Install the requirements
WORKDIR /app/atd-bond-reporting 
RUN pip install -r requirements.txt
