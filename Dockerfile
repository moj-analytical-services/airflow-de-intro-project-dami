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

COPY . .

RUN pip install -r requirements.txt


ENTRYPOINT python scripts/run.py
