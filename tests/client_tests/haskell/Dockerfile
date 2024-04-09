FROM haskell:9

RUN \
  groupadd jenkins && \
  useradd -u 1001 -g jenkins -m jenkins && \
  apt-get update && \
  apt-get -y install \
    libpq-dev \
    python3-venv \
    python3-dev \
    build-essential

ENV HOME /root
WORKDIR /root
USER jenkins

CMD ["/bin/bash"]
