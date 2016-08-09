FROM java
MAINTAINER Namrata Malarout “namrata.malarout@jpl.nasa.gov”
# Install packages
RUN apt-get update
RUN apt-get install -y yum
RUN apt-get install -y lsof

ENV SOLR solr-5.3.1
ADD solr-5.3.1.tgz /opt/
RUN ln -s /opt/$SOLR /opt/solr

EXPOSE 8983

RUN adduser solr
RUN chown -Rf solr.solr /opt
USER solr

CMD [“/bin/bash”, “-c”, “cd /opt/solr; ./bin/solr start”]
