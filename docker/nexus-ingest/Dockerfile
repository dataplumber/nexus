FROM centos:7



# To enable systemd. See https://github.com/docker-library/docs/tree/master/centos
# ENV container docker
# RUN (cd /lib/systemd/system/sysinit.target.wants/; for i in *; do [ $i == \
# systemd-tmpfiles-setup.service ] || rm -f $i; done); \
# rm -f /lib/systemd/system/multi-user.target.wants/*;\
# rm -f /etc/systemd/system/*.wants/*;\
# rm -f /lib/systemd/system/local-fs.target.wants/*; \
# rm -f /lib/systemd/system/sockets.target.wants/*udev*; \
# rm -f /lib/systemd/system/sockets.target.wants/*initctl*; \
# rm -f /lib/systemd/system/basic.target.wants/*;\
# rm -f /lib/systemd/system/anaconda.target.wants/*;
# VOLUME [ "/sys/fs/cgroup" ]

WORKDIR /tmp

RUN yum -y install wget git which

# Install Oracle JDK 1.8u121-b13
RUN wget -q --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u121-b13/e9e7ea248e2c4826b92b3f075a80e441/jdk-8u121-linux-x64.rpm"
RUN yum -y install jdk-8u121-linux-x64.rpm
ENV JAVA_HOME /usr/java/default

# ########################
# # Apache Maven   #
# ########################
ENV M2_HOME /usr/local/apache-maven
ENV M2 $M2_HOME/bin 
ENV PATH $PATH:$M2

RUN mkdir $M2_HOME
RUN wget -q http://mirror.stjschools.org/public/apache/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz && \
    tar -xvzf apache-maven-3.3.9-bin.tar.gz -C $M2_HOME --strip-components=1 && \
    rm -f apache-maven-3.3.9-bin.tar.gz

# ########################
# # Anaconda   #
# ########################
RUN wget -q https://repo.continuum.io/archive/Anaconda2-4.3.0-Linux-x86_64.sh
RUN chmod +x Anaconda2-4.3.0-Linux-x86_64.sh
RUN ./Anaconda2-4.3.0-Linux-x86_64.sh -b -p /usr/local/anaconda2
