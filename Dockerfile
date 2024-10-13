FROM apache/airflow:2.9.2
USER root
RUN apt-get update \
  && apt install wget \
  && wget https://jdbc.postgresql.org/download/postgresql-42.7.3.jar -P /opt/spark/jars/ \
  && apt install -y default-jdk \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
WORKDIR /app
COPY requirements.txt /app
RUN pip install --upgrade pip \
  && pip install --trusted-host pypi.python.org -r requirements.txt