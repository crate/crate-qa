FROM mcr.microsoft.com/dotnet/sdk:6.0

RUN \
  apt-get update && \
  apt-get -y install python3-venv python3-dev build-essential

ENV HOME /root
WORKDIR /root
USER root

CMD ["bash"]
