# Run example: docker run -it --net=host -p 8083:8083 -e MASTER=mesos://127.0.0.1:5050 nexus-webapp

FROM nexusjpl/spark-mesos-base

MAINTAINER Joseph Jacob "Joseph.Jacob@jpl.nasa.gov"

# Set environment variables.

ENV MASTER=local[1] \
    PORT_NUM=8083 \
    PATH=${CONDA_HOME}/bin:${PATH}:${HADOOP_HOME}/bin:${SPARK_HOME}/bin \
    PYTHONPATH=${PYTHONPATH}:${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.1-src.zip \
    PYSPARK_DRIVER_PYTHON=${CONDA_HOME}/bin/python \
    PYSPARK_PYTHON=${CONDA_HOME}/bin/python \
    PYSPARK_SUBMIT_ARGS="--driver-memory=4g pyspark-shell" \
    SPARK_LOCAL_IP=nexus-webapp

# Run NEXUS webapp.

EXPOSE ${PORT_NUM}

WORKDIR /tmp

CMD python -m webservice.webapp
