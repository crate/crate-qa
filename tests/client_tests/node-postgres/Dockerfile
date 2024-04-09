FROM node:15-buster

RUN \
  groupadd jenkins && \
  useradd -u 1001 -g jenkins -m jenkins && \
  apt-get update && \
  apt-get -y install python3-pip libpq5 libpq-dev sudo

ENV HOME /root
WORKDIR /root
USER jenkins

CMD ["bash"]
