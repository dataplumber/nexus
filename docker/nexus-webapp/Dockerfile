# Run example: docker run -it --net=host -p 8083:8083 -e MASTER=mesos://127.0.0.1:5050 nexus-webapp

FROM nexusjpl/spark-mesos-base

MAINTAINER Joseph Jacob "Joseph.Jacob@jpl.nasa.gov"

# Install packages needed for builds

WORKDIR /

RUN yum install -y gcc python-devel

# Set environment variables.  For Mesos, I used MESOS_VER because MESOS_VERSION
# is expected to be a logical TRUE/FALSE flag that tells Mesos whether or not 
# to simply print the version number and exit.

ENV MASTER=local[1] \
    PORT_NUM=8083 \
    CONDA_HOME=${INSTALL_LOC}/anaconda2

ENV PATH=${CONDA_HOME}/bin:${PATH}:${HADOOP_HOME}/bin:${SPARK_HOME}/bin \
    PYTHONPATH=${PYTHONPATH}:${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.1-src.zip \
    PYSPARK_DRIVER_PYTHON=${CONDA_HOME}/bin/python \
    PYSPARK_PYTHON=${CONDA_HOME}/bin/python \
    PYSPARK_SUBMIT_ARGS="--driver-memory=4g pyspark-shell" \
    HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop \
    SPARK_DIST_CLASSPATH="${HADOOP_HOME}/etc/hadoop/*:${HADOOP_HOME}/share/hadoop/common/lib/*:${HADOOP_HOME}/share/hadoop/common/*:${HADOOP_HOME}/share/hadoop/hdfs/*:${HADOOP_HOME}/share/hadoop/hdfs/lib/*:${HADOOP_HOME}/share/hadoop/hdfs/*:${HADOOP_HOME}/share/hadoop/yarn/lib/*:${HADOOP_HOME}/share/hadoop/yarn/*:${HADOOP_HOME}/share/hadoop/mapreduce/lib/*:${HADOOP_HOME}/share/hadoop/mapreduce/*:${HADOOP_HOME}/share/hadoop/tools/lib/*" \
    SPARK_LOCAL_IP=127.0.0.1 \
    MESOS_NATIVE_JAVA_LIBRARY=${INSTALL_LOC}/lib/libmesos.so \
    SPARK_EXECUTOR_URI=${INSTALL_LOC}/spark-2.1.0-bin-without-hadoop.tgz

# Set up Anaconda environment
    
RUN conda install -y numpy cython mpld3 scipy netCDF4 basemap gdal libnetcdf hdf4 matplotlib && \
#    conda install -c anaconda -y matplotlib=1.5.3 && \
    pip install shapely cython py4j

# Workaround missing libcom_err.so (needed for gdal)

RUN cd /usr/lib64 && ln -s libcom_err.so.2 libcom_err.so.3

# Workaround missing conda libs needed for gdal

RUN cd ${CONDA_HOME}/lib && \
    ln -s libnetcdf.so.11 libnetcdf.so.7 && \
    ln -s libkea.so.1.4.6 libkea.so.1.4.5 && \
    ln -s libhdf5_cpp.so.12 libhdf5_cpp.so.10

# Retrieve NEXUS code and build it.

RUN git clone https://github.com/dataplumber/nexus.git

RUN sed -i 's/,algorithms.doms//g' /nexus/analysis/webservice/config/web.ini && \
    sed -i 's/6g/1g/' /nexus/analysis/webservice/webapp.py

WORKDIR /nexus/nexus-ingest/nexus-messages

RUN ./gradlew clean build install

WORKDIR /nexus/nexus-ingest/nexus-messages/build/python/nexusproto

RUN python setup.py install

WORKDIR /nexus/data-access

RUN python setup.py install

WORKDIR /nexus/analysis

RUN python setup.py install

# Run NEXUS webapp.

EXPOSE ${PORT_NUM}

WORKDIR /tmp

CMD python -m webservice.webapp
