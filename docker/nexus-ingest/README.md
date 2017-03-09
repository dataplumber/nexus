# nexus-ingest Docker

This can be used to start spring-xd in singlenode mode with all nexus modules already installed.


The server can be started with a command like:

`docker run -it -v /Users/greguska/data/:/usr/local/data/nexus -p 9394:9393 --name nexus-ingest nexusjpl/nexus-ingest`

You can then connect to the Admin UI with http://localhost:9394/admin-ui.

Alternatively you can use the springxd shell docker image to connect via the spring xd shell with this command:

`docker run -it --network container:nexus-ingest springxd/shell`