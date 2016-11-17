# Cluster_Project_1

* HPC Twitter Data Mining
* A project for mining a large set of tweets on a multicore computing environment using MPI.

Two different sizes of twitter CSV files that were 50M and 10G, were tested to explore the behaviour of multicore system. 
Three kinds of resource configurations are utilized, which are 1 core on 1 node, 8 cores on 1 node, and 8 cores on 2 nodes. 

For a large size of 10G file, it is partitioned to chunks to be arranged to numbers of processors. 
The size of chunk is determined by the size of file and the number of processors. 
Each processor implements search tasks within its own chunk. MPI is used to handle communication between processors. 

The focal point of the execution time is how to access the given files, especially a very large one. 
In my code, I used the class BufferedReader in JAVA. Each time a fixed size of data was read in the buffer and searched. 
Once it was done, the buffer would read in new data form the file. This approach improved the runtime very effectively. 


By using MPJ express, the JAVA code could be executed in parallel. 
MPI.COMM_WORLD.Isend() was used for other processors sending messages to the main processor without blocking. 
When the main processor was proposed to receive messages, MPI.COMM_WORLD.Recv() was used to ensure blocking receive.
