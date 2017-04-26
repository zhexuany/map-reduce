include "shared.thrift"

namespace java edu.umn.cs.mapreduce

struct JobRequest {
    1: required string inputFile;
}

enum JobStatus {
    SUCCESS,
    NO_NODES_IN_CLUSTER
}

struct JobStats {
    1: required i32 chunkSize;
    2: required i32 numSplits;
    3: required i32 totalSortTasks;
    4: required i32 totalScheduledSortTasks;
    5: required i32 totalSuccessfulSortTasks;
    6: required i32 totalFailedSortTasks;
    7: required i32 totalKilledSortTasks;
    8: required i32 totalMergeTasks;
    9: required i32 totalScheduledMergeTasks;
    10: required i32 totalSuccessfulMergeTasks;
    11: required i32 totalFailedMergeTasks;
    12: required i32 totalKilledMergeTasks;
    13: required i64 averageTimeToSort;
    14: required i64 averageTimeToMerge;
    15: required i32 taskRedundancy;
    16: required double nodeFailProbability;
    17: required double taskFailProbability;
}

struct JobResponse {
    1: required JobStatus status;
    2: optional string outputFile;
    3: optional JobStats jobStats;
    4: optional i64 executionTime;
}

struct JoinResponse {
    1: required double nodeFailProbability;
    2: required double taskFailProbability;
    3: required i32 heartbeatInterval;
}

service MasterEndPoints {
    // Used by clients
    JobResponse submit(1: JobRequest request);

    // Used by slaves
    JoinResponse join(1:string hostname, 2:i32 port);
    void heartbeat(1:string hostname, 2:i32 port);
}