# docker build -t airflow_tecton .
# docker run -v /path/to/airflowconfigs/airflow:/root/airflow -p 8080:8080 airflow_tecton
FROM python:3.9-slim

RUN apt-get update \
&& apt-get install gcc python3-dev vim -y \
&& apt-get clean

RUN mkdir -p /opt/airflow_tecton/
WORKDIR /opt/airflow_tecton/

RUN pip install pydantic==1.10.9
RUN pip install pydantic-core==0.39.0
RUN pip install pandas
RUN pip install pyarrow
RUN pip install fastparquet

RUN pip install --no-cache-dir --upgrade pip && \
	pip install --no-cache-dir apache-airflow>=2.0


COPY ./ /opt/airflow_tecton

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow_tecton"

ENV AIRFLOW_HOME=/root/airflow

CMD ["airflow", "standalone"]
