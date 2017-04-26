package edu.umn.cs.mapreduce.slave;

import edu.umn.cs.mapreduce.JoinResponse;
import edu.umn.cs.mapreduce.MasterEndPoints;
import edu.umn.cs.mapreduce.SlaveEndPoints;
import edu.umn.cs.mapreduce.common.Constants;
import edu.umn.cs.mapreduce.common.Utilities;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;

/**
 * Created by zhexuany on 3/1/16.
 */

public class Slave {
    private static final Logger LOG = LoggerFactory.getLogger(Slave.class);

    private SlaveEndPointsImpl slaveEndPoints;
    private SlaveEndPoints.Processor slaveProcessor;

    private void startService(String hostname, int port, String masterHostname) {
        try {
            LOG.info("Checking for existence of intermediate dir: " + Constants.DEFAULT_INTERMEDIATE_DIR);
            File intDir = new File(Constants.DEFAULT_INTERMEDIATE_DIR);
            if (!intDir.exists()) {
                intDir.mkdir();
                LOG.info(Constants.DEFAULT_INTERMEDIATE_DIR + " directory created..");
            }
            JoinResponse joinResponse = joinWithMaster(hostname, port, masterHostname);
            LOG.info(hostname + ":" + port + " joined with master. Join response: " + joinResponse);
            if (slaveEndPoints == null) {
                slaveEndPoints = new SlaveEndPointsImpl(masterHostname, joinResponse.getHeartbeatInterval(),
                        joinResponse.getNodeFailProbability(), joinResponse.getTaskFailProbability(), hostname, port);
                slaveProcessor = new SlaveEndPoints.Processor(slaveEndPoints);
            }
            TServerTransport serverTransport = new TServerSocket(port);
            // Use this for a multi-threaded server
            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport)
                    .minWorkerThreads(100)
                    .processor(slaveProcessor));

            LOG.info("Started the slave service at port: " + port);
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private JoinResponse joinWithMaster(String hostname, int port, String masterHost) throws TException {
        LOG.info("Connecting to master at hostname: " + masterHost + " and port: " + Constants.MASTER_SERVICE_PORT);
        // join with master
        TTransport socket = new TSocket(masterHost, Constants.MASTER_SERVICE_PORT);
        socket.open();

        // create protocol for the superNodeSocket
        TProtocol protocol = new TBinaryProtocol(socket);

        // create the client for master's service
        MasterEndPoints.Client client = new MasterEndPoints.Client(protocol);
        JoinResponse joinResponse = client.join(hostname, port);
        LOG.info("Connected to master with identity, hostname: " + hostname + " port: " + port);
        socket.close();
        return joinResponse;
    }

    public static void main(String[] args) {
        try {
            final int port = Utilities.getRandomPort();
            final String hostname = InetAddress.getLocalHost().getHostName();
            final String masterHostname = args.length > 0 ? args[0] : hostname;

            final Slave slave = new Slave();
            // start file server service
            Runnable service = new Runnable() {
                public void run() {
                    slave.startService(hostname, port, masterHostname);
                }
            };

            // start the service thread
            new Thread(service).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
