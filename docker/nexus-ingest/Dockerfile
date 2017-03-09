FROM nexusjpl/nexusbase

WORKDIR /tmp

RUN yum -y install unzip

# Create conda environment and install dependencies
RUN conda create -y --name nexus-xd-python-modules python && \
    source activate nexus-xd-python-modules && \
    conda install -y scipy=0.18.1 && \
    conda install -y -c conda-forge nco=4.6.4 netcdf4=1.2.7
    
# Install Spring XD
WORKDIR /usr/local/spring-xd
RUN wget -q "http://repo.spring.io/libs-release/org/springframework/xd/spring-xd/1.3.1.RELEASE/spring-xd-1.3.1.RELEASE-dist.zip" && \
    unzip spring-xd-1.3.1.RELEASE-dist.zip && \
    rm spring-xd-1.3.1.RELEASE-dist.zip && \
    ln -s spring-xd-1.3.1.RELEASE current && \
    mkdir current/xd/lib/none
ENV PATH $PATH:/usr/local/spring-xd/current/xd/bin:/usr/local/spring-xd/current/shell/bin
COPY xd-singlenode-logback.groovy /usr/local/spring-xd/current/xd/config/xd-singlenode-logback.groovy
VOLUME ["/usr/local/spring-xd/current/xd/config"]
EXPOSE 9393

# ########################
# # nexus-ingest code   #
# ########################
WORKDIR /tmp
COPY install-custom-software.sh /tmp/install-custom-software.sh
RUN /bin/bash install-custom-software.sh
VOLUME ["/usr/local/data/nexus"]

COPY nexus-ingest.sh /tmp/nexus-ingest.sh
CMD ["/bin/bash", "nexus-ingest.sh"]