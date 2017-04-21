# Run example: docker run --net=nexus --name mesos-agent1 nexusjpl/spark-mesos-agent

FROM nexusjpl/spark-mesos-base

MAINTAINER Joseph Jacob "Joseph.Jacob@jpl.nasa.gov"

# Run a Mesos slave.

WORKDIR ${MESOS_HOME}/build

CMD ["/bin/bash", "-c", "./bin/mesos-agent.sh --master=${MESOS_MASTER_NAME}:${MESOS_MASTER_PORT} --port=${MESOS_AGENT_PORT} --work_dir=${MESOS_WORKDIR} --no-systemd_enable_support --launcher=posix --no-switch_user"]
