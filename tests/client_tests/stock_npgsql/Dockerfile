FROM mcr.microsoft.com/dotnet/sdk:8.0

RUN \
  groupadd jenkins && \
  useradd -u 1001 -g jenkins -m jenkins && \
  apt-get update && \
  apt-get -y install python3-venv python3-dev build-essential

ENV HOME /root
WORKDIR /root
USER jenkins

CMD ["bash"]
