FROM solr:6.4.1
MAINTAINER Nga Quach "Nga.T.Chung@jpl.nasa.gov"

USER root

RUN apt-get update && apt-get -y install git && rm -rf /var/lib/apt/lists/*

RUN cd / && git clone https://github.com/dataplumber/nexus.git && cp -r /nexus/data-access/config/schemas/solr/nexustiles . && rm -rf /nexus

RUN cd / && wget https://downloads.sourceforge.net/project/jts-topo-suite/jts/1.14/jts-1.14.zip && unzip jts-1.14.zip -d /jts-1.14 && rm jts-1.14.zip

USER $SOLR_USER

RUN cp /jts-1.14/lib/jts-1.14.jar /opt/solr/server/lib/jts-1.14.jar

RUN cp /jts-1.14/lib/jtsio-1.14.jar /opt/solr/server/lib/jtsio-1.14.jar

RUN cp -r /nexustiles /opt/solr/server/solr/.

VOLUME ["/opt/solr/server/solr/nexustiles/data"]
