# ARG DE_ECR 
FROM python:3.9-slim
# There are a number of other source images available:
# FROM ${DE_ECR}/python:3.9-bullseye
# FROM ${DE_ECR}/python:3.8-bullseye
# FROM ${DE_ECR}/python3.7-slim
# FROM ${DE_ECR}/python3.7
# FROM ${DE_ECR}/datascience-notebook:3.1.13
# FROM ${DE_ECR}/oraclelinux8-python:3.8

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

# COPY requirements.txt requirements.txt
# RUN pip install -r requirements.txt

# COPY scripts/ scripts/
# COPY data/ data/
# COPY docs/ docs/

# Ensures necessary permissions available to user in docker image
RUN chmod -R 777 .

COPY . .

RUN pip install -r requirements.txt


ENTRYPOINT python scripts/run.py
