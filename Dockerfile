FROM rust:1.77

RUN \
  groupadd jenkins && \
  useradd -u 1001 -g jenkins -m jenkins && \
  apt-get update && \
  apt-get -y upgrade && \
  apt-get -y --no-install-recommends install openjdk-17-jdk python3-venv python3-dev

ENV HOME /root
WORKDIR /root
USER jenkins

CMD ["bash"]
