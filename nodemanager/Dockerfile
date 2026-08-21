FROM bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8

RUN sed -i \
    -e 's|deb.debian.org|archive.debian.org|g' \
    -e 's|security.debian.org|archive.debian.org|g' \
    -e '/stretch-updates/d' \
    /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y --allow-unauthenticated \
        wget build-essential zlib1g-dev libssl-dev libffi-dev libbz2-dev liblzma-dev libsqlite3-dev libreadline-dev && \
    wget https://www.python.org/ftp/python/3.9.18/Python-3.9.18.tgz && \
    tar xzf Python-3.9.18.tgz && \
    cd Python-3.9.18 && \
    ./configure --enable-optimizations && \
    make -j$(nproc) && \
    make altinstall && \
    cd .. && rm -rf Python-3.9.18* && \
    ln -sf /usr/local/bin/python3.9 /usr/bin/python3 && \
    ln -sf /usr/local/bin/python3.9 /usr/bin/python && \
    apt-get clean && rm -rf /var/lib/apt/lists/*