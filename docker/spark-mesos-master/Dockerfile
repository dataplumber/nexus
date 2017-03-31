# Run example: docker run --net=host -p 5050:5050 spark-mesos-master

FROM nexusjpl/spark-mesos-base

MAINTAINER Joseph Jacob "Joseph.Jacob@jpl.nasa.gov"

EXPOSE 5050

# Run a Mesos master.

WORKDIR ${MESOS_HOME}/build

CMD ["/bin/bash", "-c", "./bin/mesos-master.sh --ip=${MESOS_IP} --work_dir=${MESOS_WORKDIR}"]
