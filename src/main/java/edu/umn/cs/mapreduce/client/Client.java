package edu.umn.cs.mapreduce.client;

/**
 * Created by zhexuany on 3/1/16.
 */

import edu.umn.cs.mapreduce.*;
import edu.umn.cs.mapreduce.common.Constants;
import org.apache.commons.cli.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.DecimalFormat;

/**
 * usage: client
 * -i <arg>  Input file to be sorted
 * -h <arg>  Hostname for master (default: localhost)
 * --help    Help
 */

public class Client {
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) throws TException {
        HelpFormatter formatter = new HelpFormatter();

        // arguments that can be passed to this application
        Options options = new Options();
        options.addOption("i", true, "Input file to be sorted");
        options.addOption("h", true, "Hostname for master (default:localhost)");
        options.addOption("help", false, "Help");

        // command line parser for the above options
        CommandLineParser cliParser = new GnuParser();
        try {
            CommandLine cli = cliParser.parse(options, args);

            // print help
            if (cli.hasOption("help")) {
                formatter.printHelp("client", options);
                return;
            }

            // if hostname of master is specified use it
            String hostname = Constants.DEFAULT_HOST;
            if (cli.hasOption("h")) {
                hostname = cli.getOptionValue("h");
            }

            if (!cli.hasOption("i")) {
                System.err.println("Input file name to be sorted must be specified");
                formatter.printHelp("client", options);
                return;
            }

            String inputFile = cli.getOptionValue("i");
            File file = new File(inputFile);
            if (!file.exists()) {
                System.err.println("Specified input file: " + inputFile + " does not exists.");
                formatter.printHelp("client", options);
                return;
            } else if (file.isDirectory()) {
                System.err.println("Specified input path: " + inputFile + " is a directory.");
                formatter.printHelp("client", options);
                return;
            }
            processCommand(hostname, inputFile);
        } catch (ParseException e) {

            // if wrong format is specified
            System.err.println("Invalid option.");
            formatter.printHelp("client", options);
        }
    }

    private static void processCommand(String hostname, String inputFile) throws TException {
        TTransport nodeSocket = new TSocket(hostname, Constants.MASTER_SERVICE_PORT);
        try {
            nodeSocket.open();
            TProtocol protocol = new TBinaryProtocol(nodeSocket);
            MasterEndPoints.Client client = new MasterEndPoints.Client(protocol);
            JobRequest request = new JobRequest(inputFile);
            LOG.info("Submitted request to master: " + request);
            JobResponse response = client.submit(request);
            if (response.getStatus().equals(JobStatus.SUCCESS)) {
                prettyPrint(inputFile, response);
            } else {
                LOG.info("Got failed response: " + response);
            }
        } finally {
            if (nodeSocket != null) {
                nodeSocket.close();
            }
        }
    }

    private static void prettyPrint(String inputFile, JobResponse response) {
        File input = new File(inputFile);
        float inputSizeMb = (float) input.length() / (1024 * 1024);
        File output = new File(response.getOutputFile());
        JobStats jobStats = response.getJobStats();
        float outputSizeMb = (float) output.length() / (1024 * 1024);
        float chunkSizeMb = (float) jobStats.getChunkSize() / (1024 * 1024);
        String pattern = "#.##";
        DecimalFormat decimalFormat = new DecimalFormat(pattern);
        System.out.println("--------------------------------------------------------------------------");
        System.out.println("                                  JOB STATISTICS                          ");
        System.out.println("--------------------------------------------------------------------------");
        System.out.println("Input file: \t\t\t\t\t" + input);
        System.out.println("Ouput file: \t\t\t\t\t" + output);
        System.out.println("Input file size: \t\t\t\t" + decimalFormat.format(inputSizeMb) + " MB");
        System.out.println("Output file size: \t\t\t\t" + decimalFormat.format(outputSizeMb) + " MB");
        System.out.println("Requested chunk size: \t\t\t\t" + decimalFormat.format(chunkSizeMb) + " MB");
        System.out.println("Task redundancy: \t\t\t\t" + jobStats.getTaskRedundancy());
        System.out.println("Node fail probability: \t\t\t\t" + jobStats.getNodeFailProbability());
        System.out.println("Task fail probability: \t\t\t\t" + jobStats.getTaskFailProbability());
        System.out.println("Number of splits: \t\t\t\t" + jobStats.getNumSplits());
        System.out.println("Total sort tasks: \t\t\t\t" + jobStats.getTotalSortTasks());
        System.out.println("Total scheduled sort tasks: \t\t\t" + jobStats.getTotalScheduledSortTasks());
        System.out.println("Total successful sort tasks: \t\t\t" + jobStats.getTotalSuccessfulSortTasks());
        System.out.println("Total failed sort tasks: \t\t\t" + jobStats.getTotalFailedSortTasks());
        System.out.println("Total killed sort tasks: \t\t\t" + jobStats.getTotalKilledSortTasks());
        System.out.println("Total merge tasks: \t\t\t\t" + jobStats.getTotalMergeTasks());
        System.out.println("Total scheduled merge tasks: \t\t\t" + jobStats.getTotalScheduledMergeTasks());
        System.out.println("Total successful merge tasks: \t\t\t" + jobStats.getTotalSuccessfulMergeTasks());
        System.out.println("Total failed merge tasks: \t\t\t" + jobStats.getTotalFailedMergeTasks());
        System.out.println("Total killed merge tasks: \t\t\t" + jobStats.getTotalKilledMergeTasks());
        System.out.println("Average time to sort: \t\t\t\t" + jobStats.getAverageTimeToSort() + " ms");
        System.out.println("Average time to merge: \t\t\t\t" + jobStats.getAverageTimeToMerge() + " ms");
        System.out.println("Overall execution time: \t\t\t" + response.getExecutionTime() + " ms");
        System.out.println("--------------------------------------------------------------------------");
    }
}
