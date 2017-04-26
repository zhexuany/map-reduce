package edu.umn.cs.mapreduce.master;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.util.concurrent.*;
import edu.umn.cs.mapreduce.*;
import edu.umn.cs.mapreduce.common.Constants;
import edu.umn.cs.mapreduce.common.Utilities;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhexuany on 3/1/16.
 */
public class MasterEndPointsImpl implements MasterEndPoints.Iface {
    private static final Logger LOG = LoggerFactory.getLogger(MasterEndPointsImpl.class);

    private Set<String> liveNodes;
    private Map<String, Stopwatch> heartbeatMap;
    private int chunkSize;
    private int mergeBatchSize;
    private int taskRedundancy;
    private int heartbeatInterval;
    private JoinResponse joinResponse;
    private ListeningExecutorService listeningExecutorService;
    private JobStats jobStats;
    private Set<FileSplit> sortJobs;
    private double nfp;
    private double tfp;

    public MasterEndPointsImpl(int chunkSize, int mergeFilesBatchSize, int heartbeatInterval, int taskRedundancy,
                               double nfp, double tfp) {
        this.liveNodes = new HashSet<>();
        this.heartbeatMap = new HashMap<>();
        this.chunkSize = chunkSize;
        this.mergeBatchSize = mergeFilesBatchSize;
        this.heartbeatInterval = heartbeatInterval;
        this.taskRedundancy = taskRedundancy;
        this.joinResponse = new JoinResponse(nfp, tfp, heartbeatInterval);
        this.listeningExecutorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(100));
        this.sortJobs = new HashSet<>();
        this.nfp = nfp;
        this.tfp = tfp;
    }

    @Override
    public JobResponse submit(JobRequest request) throws TException {
        long start = System.currentTimeMillis();
        this.jobStats = new JobStats();
        this.jobStats.setTaskRedundancy(taskRedundancy);
        this.jobStats.setNodeFailProbability(nfp);
        this.jobStats.setTaskFailProbability(tfp);
        if (liveNodes.isEmpty()) {
            JobResponse response = new JobResponse(JobStatus.NO_NODES_IN_CLUSTER);
            LOG.info("No nodes in cluster. Returning response: " + response);
            return response;
        }

        LOG.info("Processing request: " + request);
        List<FileSplit> fileSplits = null;
        try {
            fileSplits = computeSplits(request.getInputFile(), chunkSize);
        } catch (IOException e) {
            e.printStackTrace();
        }

        jobStats.setChunkSize(chunkSize);
        assert fileSplits != null;
        jobStats.setNumSplits(fileSplits.size());
        LOG.info("Input File: {} Chunk Size: {} Num Splits: {}", request.getInputFile(),
                chunkSize, fileSplits.size());

        jobStats.setTotalSortTasks(fileSplits.size());
        sortJobs.addAll(fileSplits);

        // total number of sort tasks that will be scheduled will be numSplits * taskRedundancy. So we have to wait for
        // that many responses. Each response received will count down this latch
        CountDownLatch sortCountDownLatch = new CountDownLatch(fileSplits.size() * taskRedundancy);
        List<SortResponse> sortResponses = scheduleSortJobs(fileSplits, sortCountDownLatch);
        if (sortResponses == null) {
            JobResponse response = new JobResponse(JobStatus.NO_NODES_IN_CLUSTER);
            LOG.info("No nodes in cluster. Returning response: " + response);
            return response;
        }
        jobStats.setTotalSuccessfulSortTasks(sortResponses.size());

        // we are done with sort, now batch the sort responses and schedule merge jobs
        MergeResponse finalMerge = batchAndScheduleMergeJobs(sortResponses, mergeBatchSize);
        if (finalMerge == null) {
            JobResponse response = new JobResponse(JobStatus.NO_NODES_IN_CLUSTER);
            LOG.info("No nodes in cluster. Returning response: " + response);
            return response;
        }
        LOG.info("Final merge response: " + finalMerge);

        // we are done with merge as well. now move the last written intermediate file to output directory
        String outputFile = moveFileToDestination(finalMerge.getIntermediateFilePath());

        // we no longer need the intermediate files, so clean them up
        Utilities.deleteAllIntermediateFiles();

        JobResponse response = new JobResponse(JobStatus.SUCCESS);
        long end = System.currentTimeMillis();
        response.setOutputFile(outputFile);
        response.setExecutionTime(end - start);
        response.setJobStats(jobStats);
        LOG.info("Job finished successfully in " + (end - start) + " ms. Sending response: " + response);
        return response;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private String moveFileToDestination(String intermediateFilePath) {
        File outDir = new File(Constants.DEFAULT_OUTPUT_DIR);
        if (!outDir.exists()) {
            outDir.mkdir();
        }
        File outPath = new File(outDir + "/output_sorted");
        File intFile = new File(intermediateFilePath);
        try {
            Files.move(intFile, outPath);
            LOG.info("Copied file from " + intFile + " to " + outPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return outPath.toString();
    }

    private MergeResponse batchAndScheduleMergeJobs(List<SortResponse> sortResponses, int mergeBatchSize) {
        List<String> filesToMerge = getAllFilesToMerge(sortResponses);
        List<List<String>> batches = batchJobs(filesToMerge, mergeBatchSize);
        LOG.info("Total files to merge: {} batchSize: {} numBatches: {}", filesToMerge.size(), mergeBatchSize,
                batches.size());
        return batchMergeJobs(batches);
    }

    public List<List<String>> batchJobs(List<String> filesToMerge, int mergeBatchSize) {
        return Lists.partition(filesToMerge, mergeBatchSize);
    }

    private MergeResponse batchMergeJobs(List<List<String>> batches) {
        // since this could be recursive account for already set merge task count as well
        int totalMergeAlready = jobStats.getTotalMergeTasks();
        totalMergeAlready += batches.size();
        jobStats.setTotalMergeTasks(totalMergeAlready);

        // total number of merge tasks that will be scheduled will be numSplits * taskRedundancy. So we have to wait for
        // that many responses. Each response received will count down this latch
        CountDownLatch mergeCountDownLatch = new CountDownLatch(batches.size() * taskRedundancy);
        List<MergeResponse> mergeResponses = scheduleMergeJobs(batches, mergeCountDownLatch);
        if (mergeResponses == null) {
            return null;
        }

        MergeResponse finalMergeResponse;
        if (mergeResponses.size() == 1) {
            finalMergeResponse = mergeResponses.get(0);
        } else {
            List<String> nextFilesToMerge = getRemainingFilesToMerge(mergeResponses);
            List<List<String>> newBatches = batchJobs(nextFilesToMerge, mergeBatchSize);
            finalMergeResponse = batchMergeJobs(newBatches);
        }
        return finalMergeResponse;
    }

    private List<MergeResponse> scheduleMergeJobs(List<List<String>> mergeBatches, CountDownLatch mergeCountDownLatch) {
        // create hashmap from List<List<String>> as removal is easy when success notifications are received
        Map<String, List<String>> mergeBatchesMap = new HashMap<>();
        for (List<String> batch : mergeBatches) {
            mergeBatchesMap.put(batch.toString(), batch);
        }

        // round robin assignment of merge jobs to all liveNodes using circular iterator
        Iterator<String> circularIterator = Iterables.cycle(liveNodes).iterator();
        List<MergeResponse> mergeResponses = new ArrayList<>();
        List<List<String>> failedBatches = new ArrayList<>();

        for (List<String> mergeBatch : mergeBatches) {
            Set<String> scheduledHosts = new HashSet<>();
            if (!enoughNodes()) {
                // not enough nodes to schedule
                return null;
            }
            for (int i = 0; i < taskRedundancy; i++) {
                String hostInfo = circularIterator.next();
                while (!isAlive(hostInfo)) {
                    LOG.info(hostInfo + " is not alive. Removing from live node list.");
                    circularIterator.remove();
                    if (!enoughNodes()) {
                        // not enough nodes to schedule
                        return null;
                    }
                    hostInfo = circularIterator.next();
                }

                scheduledHosts.add(hostInfo);
            }

            LOG.info("Scheduling MERGE for " + mergeBatch + " on " + scheduledHosts);
            for (String hostInfo : scheduledHosts) {
                String[] tokens = hostInfo.split(":");
                ListenableFuture<MergeResponse> future = listeningExecutorService.submit(new MergeRequestThread(tokens[0],
                        Integer.parseInt(tokens[1]), mergeBatch));
                Futures.addCallback(future, new MergeResponseListener(mergeBatch, mergeCountDownLatch, mergeResponses,
                                mergeBatchesMap, tokens[0], Integer.parseInt(tokens[1]), scheduledHosts),
                        Executors.newSingleThreadExecutor());

                // increment scheduled merge jobs count
                int scheduledAlready = jobStats.getTotalScheduledMergeTasks();
                scheduledAlready += 1;
                jobStats.setTotalScheduledMergeTasks(scheduledAlready);
            }
        }

        LOG.info("Submitted all merge requests. #requests: " + mergeBatches.size());
        try {
            // we have to wait until the count down becomes 0 (i.e; all merge responses received)
            mergeCountDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // if there are some merge jobs that are not removed from this map then it means those are failed jobs
        if (!mergeBatchesMap.isEmpty()) {
            failedBatches.addAll(mergeBatchesMap.values());
        }

        // if there are any failed responses then reschedule those requests again
        if (!failedBatches.isEmpty()) {
            // since this could be recursive account for already set successful merge task count as well
            int totalFailedAlready = jobStats.getTotalFailedMergeTasks();
            totalFailedAlready += failedBatches.size();
            jobStats.setTotalFailedMergeTasks(totalFailedAlready);

            LOG.info(failedBatches.size() + " merge requests FAILED. Rescheduling the failed requests..");
            mergeCountDownLatch = new CountDownLatch(failedBatches.size());
            List<MergeResponse> responsesForFailedTasks = scheduleMergeJobs(failedBatches, mergeCountDownLatch);
            if (responsesForFailedTasks == null) {
                return null;
            }
            mergeResponses.addAll(responsesForFailedTasks);
        }

        long totalMergeTime = 0;
        for (MergeResponse mergeResponse : mergeResponses) {
            totalMergeTime += mergeResponse.getExecutionTime();
        }
        long avgMergeTime = totalMergeTime / mergeResponses.size();
        jobStats.setAverageTimeToMerge(avgMergeTime);

        // since this could be recursive account for already set successful merge task count as well
        int totalSuccessAlready = jobStats.getTotalSuccessfulMergeTasks();
        totalSuccessAlready += mergeResponses.size();
        jobStats.setTotalSuccessfulMergeTasks(totalSuccessAlready);

        LOG.info("Got all merge responses back. #responses: " + mergeResponses.size());
        return mergeResponses;
    }

    private List<String> getAllFilesToMerge(List<SortResponse> sortResponses) {
        List<String> result = new ArrayList<>();
        for (SortResponse sortResponse : sortResponses) {
            result.add(sortResponse.getIntermediateFilePath());
        }
        return result;
    }

    private List<String> getRemainingFilesToMerge(List<MergeResponse> mergeResponses) {
        List<String> result = new ArrayList<>();
        for (MergeResponse mergeResponse : mergeResponses) {
            result.add(mergeResponse.getIntermediateFilePath());
        }
        return result;
    }

    private List<SortResponse> scheduleSortJobs(List<FileSplit> fileSplits, CountDownLatch sortCountDownLatch) {
        // round robin assignment of sort jobs to all liveNodes
        Iterator<String> circularIterator = Iterables.cycle(liveNodes).iterator();
        List<FileSplit> failedSplits = new ArrayList<>();
        List<SortResponse> sortResponses = new ArrayList<>();

        for (FileSplit fileSplit : fileSplits) {
            Set<String> scheduledHosts = new HashSet<>();

            if (!enoughNodes()) {
                // not enough nodes to schedule
                return null;
            }
            for (int i = 0; i < taskRedundancy; i++) {
                String hostInfo = circularIterator.next();
                while (!isAlive(hostInfo)) {
                    LOG.info(hostInfo + " is not alive. Removing from live node list.");
                    circularIterator.remove();
                    if (!enoughNodes()) {
                        // not enough nodes to schedule
                        return null;
                    }
                    hostInfo = circularIterator.next();
                }
                scheduledHosts.add(hostInfo);
            }

            LOG.info("Scheduling SORT for " + fileSplit + " on " + scheduledHosts);
            for (String hostInfo : scheduledHosts) {
                String[] tokens = hostInfo.split(":");
                ListenableFuture<SortResponse> future = listeningExecutorService.submit(new SortRequestThread(tokens[0],
                        Integer.parseInt(tokens[1]), fileSplit));
                Futures.addCallback(future, new SortResponseListener(fileSplit, sortCountDownLatch, sortResponses,
                                sortJobs, tokens[0], Integer.parseInt(tokens[1]), scheduledHosts),
                        Executors.newSingleThreadExecutor());

                // increment scheduled sort jobs count
                int scheduledAlready = jobStats.getTotalScheduledSortTasks();
                scheduledAlready += 1;
                jobStats.setTotalScheduledSortTasks(scheduledAlready);
            }
        }
        LOG.info("Submitted all sort requests. #requests: " + fileSplits.size() + " #taskRedundancy: " + taskRedundancy);

        try {
            // we have to wait until the count down becomes 0 (i.e; all sort responses received)
            sortCountDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // if sortJobs is not empty then it means there are some failed jobs that needs to rescheduled
        if (!sortJobs.isEmpty()) {
            failedSplits.addAll(sortJobs);
        }

        if (!failedSplits.isEmpty()) {
            // since this can be recursive, account for previous failed sort tasks as well
            int failedAlready = jobStats.getTotalFailedSortTasks();
            failedAlready += failedSplits.size();
            jobStats.setTotalFailedSortTasks(failedAlready);

            LOG.info(failedSplits.size() + " sort requests FAILED. Rescheduling the failed requests..");
            sortCountDownLatch = new CountDownLatch(failedSplits.size() * taskRedundancy);
            List<SortResponse> responsesForFailedTasks = scheduleSortJobs(failedSplits, sortCountDownLatch);
            if (responsesForFailedTasks == null) {
                return null;
            }
            sortResponses.addAll(responsesForFailedTasks);
        }

        long totalSortTime = 0;
        for (SortResponse sortResponse : sortResponses) {
            totalSortTime += sortResponse.getExecutionTime();
        }
        long avgSortTime = totalSortTime / sortResponses.size();
        jobStats.setAverageTimeToSort(avgSortTime);

        LOG.info("Got all sort responses back. #responses: " + sortResponses.size());
        return sortResponses;
    }

    private boolean enoughNodes() {
        if (liveNodes.size() < taskRedundancy) {
            LOG.info("At least " + taskRedundancy + " nodes should be alive for proactive fault tolerance" +
                    " with task redundancy of " + taskRedundancy);
            return false;
        }
        return true;
    }

    private boolean isAlive(String hostInfo) {
        Stopwatch stopwatch = heartbeatMap.get(hostInfo);
        long elapsedTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        // if elapsed time of this node's stopwatch is greater than that of heartbeat interval, let's assume the
        // node to be dead and not schedule any tasks to it
        // NOTE: sometimes even when the node is alive, stop watch elapsed time gets slightly greater (maybe because of
        // network delays) which wrongly assumes the node to be dead. To account for this we add 200ms more for the
        // aliveness condition.
        if (elapsedTime > heartbeatInterval + 200) {
            LOG.info("Stopwatch elapsed time: {} ms for node: {} is greater than heartbeat interval: {}",
                    elapsedTime, hostInfo, heartbeatInterval);
            LOG.info("Assuming host: {} to be DEAD", hostInfo);
            return false;
        }
        return true;
    }

    public List<FileSplit> computeSplits(String inputFilename, long chunkSize) throws IOException {
        List<FileSplit> splits = new ArrayList<>();
        File inputFile = new File(inputFilename);
        if (!inputFile.exists()) {
            LOG.info("file does not exist.");
            return splits;
        }
        long fileLen = inputFile.length();
        int expectedSplits;
        // if file length and chunk size are exactly divisible
        if (fileLen % chunkSize == 0) {
            expectedSplits = (int) (fileLen / chunkSize);
        } else {
            expectedSplits = (int) ((fileLen / chunkSize) + 1);
        }

        long offset = 0;
        RandomAccessFile randomAccessFile = new RandomAccessFile(inputFile, "r");
        for (int i = 0; i < expectedSplits; i++) {
            long adjustedLength = chunkSize;
            try {
                // seek into the file and read the value. Read until we encounter a space of EOF
                randomAccessFile.seek(offset + adjustedLength);
                char charAtOffset = (char) randomAccessFile.readByte();
                while (charAtOffset != ' ') {
                    adjustedLength++;
                    try {
                        charAtOffset = (char) randomAccessFile.readByte();
                    } catch (EOFException eof) {
                        // ignore as we reached EOF
                        break;
                    }
                }
            } catch (EOFException eof) {
                // ignore as we reached EOF
            }
            LOG.info("Adding split from offset: {} length: {} for file: {}", offset, adjustedLength,
                    inputFile.getAbsolutePath());
            splits.add(new FileSplit(inputFile.getAbsolutePath(), offset, adjustedLength));
            offset += adjustedLength;
        }
        randomAccessFile.close();
        return splits;
    }

    @Override
    public JoinResponse join(String hostname, int port) throws TException {
        String hostInfo = hostname + ":" + port;
        liveNodes.add(hostInfo);
        LOG.info(hostInfo + " joined the cluster. Returning response: " + joinResponse);
        return joinResponse;
    }

    @Override
    public void heartbeat(String hostname, int port) throws TException {
        String hostInfo = hostname + ":" + port;
        if (heartbeatMap.containsKey(hostInfo)) {
            Stopwatch stopwatch = heartbeatMap.get(hostInfo);
            stopwatch.reset();
            stopwatch.start();
        } else {
            Stopwatch stopwatch = Stopwatch.createStarted();
            heartbeatMap.put(hostInfo, stopwatch);
            LOG.info("Received first heartbeat from " + hostInfo);
        }
    }

    private class MergeResponseListener implements FutureCallback<MergeResponse> {
        private List<String> mergeBatch;
        private CountDownLatch mergeCountDownLatch;
        private List<MergeResponse> mergeResponses;
        private final Map<String, List<String>> mergeJobs;
        private Set<String> otherHosts;
        private String thisHost;

        public MergeResponseListener(List<String> mergeBatch, CountDownLatch mergeCountDownLatch,
                                     List<MergeResponse> mergeResponses, Map<String, List<String>> mergeJobs,
                                     String host, int port, Set<String> scheduledHosts) {
            this.mergeBatch = mergeBatch;
            this.mergeCountDownLatch = mergeCountDownLatch;
            this.mergeResponses = mergeResponses;
            this.mergeJobs = mergeJobs;
            // since scheduledHosts list is shared across multiple listeners, we make a copy and remove own host+port
            // from the list (create other nodes list which will be used for kill job)
            this.otherHosts = new HashSet<>(scheduledHosts);
            this.thisHost = host + ":" + port;
            this.otherHosts.remove(thisHost);
        }

        @Override
        public void onSuccess(MergeResponse mergeResponse) {
            if (mergeResponse.getStatus().equals(Status.SUCCESS)) {
                // since mergeJobs list is shared across multiple listeners, it needs to be synchronized.
                // First listener receiving SUCCESS notification, will remove the current job from mergeJobs list,
                // it will also issue kill job command to all other hosts running the same job
                synchronized (mergeJobs) {
                    if (mergeJobs.containsKey(mergeBatch.toString())) {
                        mergeJobs.remove(mergeBatch.toString());
                        mergeResponses.add(mergeResponse);
                        for (String hostInfo : otherHosts) {
                            String[] tokens = hostInfo.split(":");
                            String host = tokens[0];
                            int port = Integer.valueOf(tokens[1]);
                            try (TTransport socket = new TSocket(host, port)) {
                                socket.open();
                                TProtocol protocol = new TBinaryProtocol(socket);
                                SlaveEndPoints.Client client = new SlaveEndPoints.Client(protocol);
                                client.killMerge(mergeBatch);
                                // increment kill task count
                                int killedAlready = jobStats.getTotalKilledMergeTasks();
                                killedAlready += 1;
                                jobStats.setTotalKilledMergeTasks(killedAlready);
                            } catch (TException e) {
                                // This node might have failed and connection will be refused, so ignore exception
                            }
                        }
                    }
                }
            }
            mergeCountDownLatch.countDown();
            LOG.info("Remaining merge jobs: " + mergeCountDownLatch.getCount());
        }

        @Override
        public void onFailure(Throwable throwable) {
            // ignore as MergeResponse will have failure status
        }
    }

    private class SortResponseListener implements FutureCallback<SortResponse> {
        private FileSplit fileSplit;
        private CountDownLatch sortCountDownLatch;
        private List<SortResponse> sortResponses;
        private final Set<FileSplit> sortJobs;
        private String thisHost;
        private Set<String> otherHosts;

        public SortResponseListener(FileSplit fileSplit, CountDownLatch sortCountDownLatch,
                                    List<SortResponse> sortResponses, Set<FileSplit> sortJobs,
                                    String host, int port, Set<String> scheduledHosts) {
            this.fileSplit = fileSplit;
            this.sortCountDownLatch = sortCountDownLatch;
            this.sortResponses = sortResponses;
            this.sortJobs = sortJobs;
            // since scheduledHosts list is shared across multiple listeners, we make a copy and remove own host+port
            // from the list (create other nodes list which will be used for kill job)
            this.otherHosts = new HashSet<>(scheduledHosts);
            this.thisHost = host + ":" + port;
            this.otherHosts.remove(thisHost);
        }

        @Override
        public void onSuccess(SortResponse sortResponse) {
            if (sortResponse.getStatus().equals(Status.SUCCESS)) {
                // since sortJobs list is shared across multiple listeners, it needs to be synchronized.
                // First listener receiving SUCCESS notification, will remove the current job from sortJobs list,
                // it will also issue kill job command to all other hosts running the same job
                synchronized (sortJobs) {
                    if (sortJobs.contains(fileSplit)) {
                        sortJobs.remove(fileSplit);
                        sortResponses.add(sortResponse);
                        for (String hostInfo : otherHosts) {
                            String[] tokens = hostInfo.split(":");
                            String host = tokens[0];
                            int port = Integer.valueOf(tokens[1]);
                            try (TTransport socket = new TSocket(host, port)) {
                                socket.open();
                                TProtocol protocol = new TBinaryProtocol(socket);
                                SlaveEndPoints.Client client = new SlaveEndPoints.Client(protocol);
                                client.killSort(fileSplit);
                                // increment kill task count
                                int killedAlready = jobStats.getTotalKilledSortTasks();
                                killedAlready += 1;
                                jobStats.setTotalKilledSortTasks(killedAlready);
                            } catch (TException e) {
                                // This node might have failed and connection will be refused, so ignore exception
                            }
                        }
                    }
                }
            }
            sortCountDownLatch.countDown();
            LOG.info("Remaining sort jobs: " + sortCountDownLatch.getCount());
        }

        @Override
        // ignore as SortResponse will have failure status
        public void onFailure(Throwable throwable) {
        }
    }
}
