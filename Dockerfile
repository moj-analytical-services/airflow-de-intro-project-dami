# ARG DE_ECR 
FROM python:3.9-slim

# Update apt package lists
RUN apt-get update

# Install Git
RUN apt-get install -y git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Verify the Git installation
RUN git --version

# Create a working directory to do stuff from
WORKDIR /etl

# Ensures necessary permissions available to user in docker image
RUN chmod -R 777 .

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT python scripts/run.py
