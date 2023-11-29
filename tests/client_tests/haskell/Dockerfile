FROM haskell:9

RUN \
  apt-get update && \
  apt-get -y install \
    libpq-dev \
    python3-venv \
    python3-dev \
    build-essential

ENV HOME /root
WORKDIR /root
USER root

CMD ["/bin/bash"]
