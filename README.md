# SDIS

We recommend testing the application inside the script/ folder.
Simply enter the script/ folder and run

    ./compile

Consider editing ./launch-peer and ./snooper before proceeding.

To launch a peer, simply run:

    ./launch-peer ID

and the peer's access point will be the same as its ID.

To create a file with random contents:

    ./randomfile SIZE NAME

Shortcuts of TestApp invocations:

    ./backup ID FILE DESIRED_REPLICATION_DEGREE
    ./restore ID FILE
    ./reclaim ID AMOUNT
    ./delete ID FILE
    ./delete ID FILE -o
    ./state ID

> The -o option of the _delete_ command is used to request the enhanced version of the _File Deletion_ sub-protocol

### RMI registry

The rmiregistry must be started outside the application. One way is to run

    CLASSPATH=. rmiregistry &

inside the script/ folder before launching the peers.

### Test files

Press enter when prompted and when all messaging is finished.
Consider erasing the /tmp/dbs (or whatever Configuration.allPeersRootDir has been set to) between tests.
The peers will recover the metadata if run on the same directory (aka with the same id).

#### Roundabout

For 4 peers, launches a backup for a 500KB file with desired replication degree of 3 on peer 1,
meaning all of the other three peers must back up the file's chunks to reach the desired replication.
Then, it cuts peer 2's allowance to 200KB, meaning it must remove some chunks. The other peers will
initiate PUTCHUNK subprotocol instances, which will only succeed 2s later when peer 2's allowance
is raised back to 5000KB, meaning it can once more store the multicasted chunks:

    ./roundabout

#### Pipeline

For 5 peers, performs a pipeline of 5 backups, then 5 restores, then 5 deletes, and ensures each peer
reconstructed its file correctly and ended up with 0 backup space utilized. For the reclaim parameters
given, and depending on which chunks the peers choose to store, it may be that some chunks will not reach
their desired replication degree, but this does not affect the restores or deletes:

    ./pipeline

#### Auto-pipeline

Like ./pipeline but it launches the peers for us. We suggest using ./pipeline instead, this can get quite incomprehensible.

    ./auto-pipeline

#### Launch the Peers and TestApp explicitly

Use "java -cp ." or add the script directly to the classpath with one of

    export CLASSPATH=.
    export CLASSPATH="$CLASSPATH:."

To launch the peer explicitly:

    java dbs.Peer <protocol_version> <server_id> <access_point> <mc_address> <mc_port> <mdb_address> <mdb_port> <mdr_address> <mdr_port>

To launch the TestApp explicitly:

    java dbs.TestApp <arguments>
