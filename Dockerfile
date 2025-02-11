#GO 1.22.2
FROM golang:1.22.2-bullseye

# Install build dependencies
RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    libgflags-dev \
    libsnappy-dev \
    zlib1g-dev \
    libbz2-dev \
    liblz4-dev \
    libzstd-dev \
    && rm -rf /var/lib/apt/lists/*

# Clone 9.8 RocksDB as it works with v1.9.8
RUN git clone -b 9.8.fb https://github.com/facebook/rocksdb.git /rocksdb \
    && cd /rocksdb \
    && make shared_lib \
    && cp librocksdb.* /usr/lib/ \
    && cp -r include/rocksdb /usr/include/

# Set working directory
WORKDIR /app

# Copy go.mod and go.sum first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the application
COPY . .

# For RocksDB
ENV CGO_CFLAGS="-I/usr/include"
ENV CGO_LDFLAGS="-L/usr/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"
ENV LD_LIBRARY_PATH="/usr/lib"


CMD ["/bin/sh", "-C", "runner.sh"]