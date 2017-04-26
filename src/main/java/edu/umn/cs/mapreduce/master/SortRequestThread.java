package edu.umn.cs.mapreduce.master;

import edu.umn.cs.mapreduce.FileSplit;
import edu.umn.cs.mapreduce.SlaveEndPoints;
import edu.umn.cs.mapreduce.SortResponse;
import edu.umn.cs.mapreduce.Status;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.concurrent.Callable;

/**
 * Created by zhexuany on 3/1/16.
 */

public class SortRequestThread implements Callable<SortResponse> {
    private String slaveHost;
    private int slavePort;
    private FileSplit fileSplit;

    public SortRequestThread(String slaveHost, int slavePort, FileSplit fileSplit) {
        this.slaveHost = slaveHost;
        this.slavePort = slavePort;
        this.fileSplit = fileSplit;
    }

    @Override
    public SortResponse call() throws TException {
        TTransport socket = new TSocket(slaveHost, slavePort);
        SortResponse sortResponse = null;
        try {
            socket.open();
            TProtocol protocol = new TBinaryProtocol(socket);
            SlaveEndPoints.Client client = new SlaveEndPoints.Client(protocol);
            sortResponse = client.sort(fileSplit);
        } catch (TTransportException e) {
            // This can happen if connection is refused or node is manually killed when sort is executing
            return new SortResponse(Status.FAILED);
        }
        return sortResponse;
    }
}
