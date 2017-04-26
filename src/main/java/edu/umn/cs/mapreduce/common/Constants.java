package edu.umn.cs.mapreduce.common;

/**
 * Created by zhexuany on 4/16/16.
 */
public class Constants {
    public static final String DEFAULT_HOST = "localhost";
    public static final int MASTER_SERVICE_PORT = 9090;
    public static final int DEFAULT_CHUNK_SIZE = 1024 * 1024; // 1MB
    public static final int DEFAULT_MERGE_BATCH_SIZE = 8;
    public static final int DEFAULT_TASK_REDUNDANCY = 1;
    public static final int HEARTBEAT_INTERVAL = 500;
    public static final double DEFAULT_NODE_FAIL_PROBABILITY = 0.0;
    public static final double DEFAULT_TASK_FAIL_PROBABILITY = 0.1;
    public static final String DEFAULT_INTERMEDIATE_DIR = "./intermediate_dir";
    public static final String DEFAULT_OUTPUT_DIR = "./output_dir";
}
