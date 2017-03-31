# Run example: docker run --net=host -e MESOS_AGENT_PORT=5051 spark-mesos-agent
# docker run --net=host -e MESOS_AGENT_PORT=5053 -e MESOS_IP=128.149.122.201 spark-mesos-agent

FROM spark-mesos-base

MAINTAINER Joseph Jacob "Joseph.Jacob@jpl.nasa.gov"

# Run a Mesos slave.

ENV MESOS_AGENT_PORT=5050

WORKDIR ${MESOS_HOME}/build

#CMD ["/bin/bash", "-c", "./bin/mesos-agent.sh --master=${MESOS_IP}:${MESOS_PORT} --port=${MESOS_AGENT_PORT} --work_dir=${MESOS_WORKDIR} --no-systemd_enable_support --launcher=posix --no-switch_user --resources='cpus:2;mem:3000;disk:5000'"]
CMD ["/bin/bash", "-c", "./bin/mesos-agent.sh --master=${MESOS_IP}:${MESOS_PORT} --port=${MESOS_AGENT_PORT} --work_dir=${MESOS_WORKDIR} --no-systemd_enable_support --launcher=posix --no-switch_user"]
