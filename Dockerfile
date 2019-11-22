FROM rust:latest

RUN \
  apt-get update && \
  apt-get -y upgrade && \
  apt-get -y install openjdk-11-jdk python3-venv

ENV HOME /root
WORKDIR /root
USER root

CMD ["bash"]
