FROM rust:1.72

RUN \
  apt-get update && \
  apt-get -y upgrade && \
  apt-get -y --no-install-recommends install openjdk-17-jdk python3-venv python3-dev

ENV HOME /root
WORKDIR /root
USER root

CMD ["bash"]
