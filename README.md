# MapReduce
Distributed sort and merge implementation with fault tolerance (also supports
proactive fault tolerance)

How To Compile:
--------------
1) To compile this project simply run compile.sh script found in root directory.
2) After the build is successful, use the following instructions to run the program

Scripts for launching different components
------------------------------------------
Following 3 scripts are used for launching different services. Use the scripts in
same following order
1) master
2) slave
3) client

Options for master
------------------
master script should be run first before running any scripts. The usage for
master script is

usage: master
 -cs <arg>   Chunk size in bytes (default: 1048576)
 -hi <arg>   Heartbeat interval in milliseconds (default: 500)
 -bs <arg>   Batch size for merge operation (default: 8)
 -tr <arg>   Task redundancy for proactive fault tolerance (default: 1)
 -nfp <arg>  Fail probability for nodes (default: 0.0)
 -tfp <arg>  Fail probability for tasks (default: 0.1)
 -h          Help

To run with defaults,
~~~
> ./scripts/master
~~~
To run with different chunk size and different merge batch size
~~~
> ./scripts/master -cs 2000000 -bs 4
~~~
To run with different fail probability for nodes 
(Assumption here is all nodes have same probability of failure)
~~~
> ./scripts/master -nfp 0.05
~~~

To enable proactive fault tolerance with 2 task for each sort and merge
~~~
> ./scripts/master -tr 2
~~~
To use different heartbeat interval and node fail probability
NOTE: Keeping the interval short may result in nodes dying quickly before
starting the client and running the tests. So its recommended to increase
the hearbeat interval or decrease the fail probability to give some time
for the nodes to run.
~~~
> ./scripts/master -hi 3000 -nfp 0.1
~~~
Since using automatic node fail probability can result in a condition
where all nodes are dead, to avoid that task fail probability can be
specified. By default, 10% of all tasks (sort and merge) could
fail to simulate fault tolerance. Using task fail probability is
more reliable than using node fail probability as with node failures
the nodes won't come back.
To run with task fail probability and task redundancy
~~~
> ./scripts/master -tr 2 -tfp 0.2 
~~~

Options for slave
-----------------
slave script accepts one optional argument i.e, hostname of master. 
If no argument is specified, by default localhost will be used for master. 
This script can be run multiple times on a single host (will use different
ports for every run) or can be run on different machines. When master
and slave scripts are run on different machines it is mandatory to provide
the hostname for master

To run (master running on same host),
~~~
> ./scripts/slave
~~~
To run when master running on different host,
~~~
> ./scripts/slave <hostname-for-master>
~~~

Options for client
------------------
client script used to trigger the job that sorts and merges the input file.

Following is the usage guide for client script.

~~~
usage: client
 -i <arg>  Input file to be sorted
 -h <arg>  Hostname for master (default: localhost)
 --help    Help
~~~

To run the client, specify the required input file argument
~~~
> ./scripts/client -i <path-to-input-file>
~~~
To run the client with master running on different host
~~~
> ./scripts/client -i <path-to-input-file> -h <hostname-for-master>
~~~

Running the client will print sample output like below

~~~
./scripts/client -i input_dir/20000000
~~~
17:04:54.350 [main] INFO  edu.umn.cs.mapreduce.client.Client -
 Submitted request to master: JobRequest(inputFile:input_dir/20000000)
~~~
--------------------------------------------------------------------------
                                  JOB STATISTICS
--------------------------------------------------------------------------
Input file:                             input_dir/20000000
Ouput file:                             ./output_dir/output_sorted
Input file size:                        93.25 MB
Output file size:                       93.25 MB
Requested chunk size:                   1 MB
Task redundancy:                        2
Node fail probability:                  0.05
Task fail probability:                  0.2
Number of splits:                       94
Total sort tasks:                       94
Total scheduled sort tasks:             192
Total successful sort tasks:            94
Total failed sort tasks:                2
Total killed sort tasks:                94
Total merge tasks:                      15
Total scheduled merge tasks:            36
Total successful merge tasks:           18
Total failed merge tasks:               3
Total killed merge tasks:               16
Average time to sort:                   195 ms
Average time to merge:                  23898 ms
Overall execution time:                 55310 ms
--------------------------------------------------------------------------
~~~

Explanation of task counts
--------------------------
Input file: Specified input file

Output file: Location of sorted output file

Input file size: Input file size in MB

Output file size: Sorted output file size in MB

Requested chunk size: Chunk size requested for split computation in MB

Task redundancy: Total tasks to launch for unit of work. In the above
example task redundancy 2 means, 2 tasks will be launched for each split
or merge job, whichever comes first will be used and other will be killed.

Node fail probability: Probability with which a node can fail. Nodes never
come back after failure.

Task fail probability: Probability with which a submitted task (sort or merge)
will fail.

Number of splits: Input file size divided by chunk size

Total sort tasks: Equal to number of splits as we need to sort all splits

Total scheduled sort tasks: If proactive fault tolerance is enabled, this
is product of task redundancy and total sort tasks (in the above example
the task redundancy is 2) and failed tasks that are
rescheduled. In above example,
2 (redundancy) * 94 (sort tasks) + 2 (redundancy) * 2 (failed sort task) = 192

Total successful sort tasks: Successfully completed sort tasks

Total failed sort tasks: Failed sort tasks (due to task or node failure)

Total killed sort tasks: Redundant tasks killed by master after receiving
output from one node

Similarly for merge tasks.

Average time to sort: Average execution time for sort tasks

Average time to merge: Average execution time for merge tasks

Overall execution time: Time between when client submits job to end of job

