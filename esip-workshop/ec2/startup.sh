#! /bin/bash

docker run -d -p 8000:8888 -v /home/ndeploy/workshop1/:/home/jovyan --network infrastructure_nexus --name jupyter nexusjpl/jupyter start-notebook.sh --NotebookApp.token='' --NotebookApp.allow_origin='*'