# Use official Python 3.10 slim image
FROM python:3.10-slim

# Install required system packages
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    gcc \
    g++ \
    libssl-dev \
    python3-dev \
    git \
    make \
    && rm -rf /var/lib/apt/lists/*

# Install custom CMake (3.22)
RUN curl -LO https://github.com/Kitware/CMake/releases/download/v3.22.0/cmake-3.22.0-linux-x86_64.sh && \
    mkdir /opt/cmake && \
    sh cmake-3.22.0-linux-x86_64.sh --prefix=/opt/cmake --skip-license && \
    ln -s /opt/cmake/bin/* /usr/local/bin/ && \
    rm cmake-3.22.0-linux-x86_64.sh

# Install Python packages (including gqlalchemy and pymgclient)
RUN pip install gqlalchemy[docker] pymgclient uv

# Set working directory
WORKDIR /app
