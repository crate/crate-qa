FROM python:3.11-slim-bullseye

ADD requirements.txt .
RUN pip install --requirement=requirements.txt

ENV HOME /root
WORKDIR /root

# Work around that CrateDB rejects being started as root.
# org.elasticsearch.bootstrap.StartupException: java.lang.RuntimeException: can not run crate as root
RUN useradd -m runner
USER runner

CMD ["bash", "run.sh"]
