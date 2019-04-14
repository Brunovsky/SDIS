# SDIS

In order to compile the application simply run the following command inside the src folder:
```console
javac -cp $CLASSPATH:../src/ -d ../classes dbs/*/*.java
```
Then, use call the Peer and TestApp classes inside the classes folder:
```console
java dbs.Peer <protocol_version> <server_id> <access_point> <mc_address> <mc_port> <mdb_address> <mdb_port> <mdr_address> <mdr_port>
java dbs.TestApp <arguments>
```

The TestApp arguments are provided according to the client's request and are its format is specified in the project's specification.
