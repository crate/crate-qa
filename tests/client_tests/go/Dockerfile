FROM golang:1.21

RUN \
  groupadd jenkins && \
  useradd -u 1001 -g jenkins -m jenkins && \
  apt-get update && \
  apt-get -y upgrade && \
  apt-get -y install python3-venv python3-dev

ENV HOME /root
WORKDIR /root
USER jenkins

CMD ["bash"]
