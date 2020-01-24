FROM openjdk:13

RUN \
  yum install deltarpm -y && \
  yum update -y && \
  yum install python3 -y && \
  yum install -y gcc-c++ make && \
  curl -sL https://rpm.nodesource.com/setup_13.x | bash - && \
  yum install nodejs -y && \
  useradd -ms /bin/bash crate

ENV HOME /home/crate
WORKDIR /home/crate
USER crate

CMD ["bash"]
