#! /bin/bash

docker run -d -p 8000:8888 -v /Users/greguska/githubprojects/nexus/esip-workshop/student-material/:/home/jovyan --network infrastructure_nexus --name jupyter nexusjpl/jupyter start-notebook.sh --NotebookApp.token='' --NotebookApp.allow_origin='*'