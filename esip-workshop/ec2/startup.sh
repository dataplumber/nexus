#! /bin/bash

cd /home/ndeploy/nexus/esip-workshop/docker/infrastructure
docker-compose up -d cassandra1

sleep 15

docker-compose up -d

sleep 30

cd /home/ndeploy/nexus/esip-workshop/docker/analysis
docker-compose up -d

docker run -d -p 8000:8888 -v /home/ndeploy/nexus/esip-workshop/student-material:/home/jovyan --network infrastructure_nexus --name jupyter nexusjpl/jupyter start-notebook.sh --NotebookApp.password='sha1:1b994085c83b:05d658b3866739ee07f5cb52a67474cdc898840e' --NotebookApp.allow_origin='*'