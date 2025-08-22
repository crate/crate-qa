FROM node:24-trixie

RUN \
  groupadd jenkins && \
  useradd -u 1001 -g jenkins -m jenkins && \
  apt-get update && \
  apt-get -y install python3-pip python3-venv libpq5 libpq-dev sudo

WORKDIR /home/jenkins
USER jenkins

CMD ["bash"]
