FROM nexusjpl/ingest-base

USER root
RUN yum install -y https://archive.cloudera.com/cdh5/one-click-install/redhat/7/x86_64/cloudera-cdh-5-0.x86_64.rpm && \
    yum install -y zookeeper

COPY create-stream-def.sh /usr/local/create-stream-def.sh

USER springxd
ENTRYPOINT ["/usr/local/nexus-ingest.sh"]
CMD ["--admin"]