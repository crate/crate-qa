FROM mcr.microsoft.com/dotnet/core/sdk:3.1

RUN \
  apt-get update && \
  apt-get -y upgrade && \
  apt-get -y install python3-venv python3-dev

ENV HOME /root
WORKDIR /root
USER root

CMD ["bash"]
