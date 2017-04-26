package edu.umn.cs.mapreduce.master;

import edu.umn.cs.mapreduce.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by zhexuany on 3/1/16.
 */

public class MergeRequestThread implements Callable<MergeResponse> {
    private String slaveHost;
    private int slavePort;
    private List<String> filesToMerge;

    public MergeRequestThread(String slaveHost, int slavePort, List<String> filesToMerge) {
        this.slaveHost = slaveHost;
        this.slavePort = slavePort;
        this.filesToMerge = filesToMerge;
    }

    @Override
    public MergeResponse call() throws TException {
        TTransport socket = new TSocket(slaveHost, slavePort);
        MergeResponse mergeResponse;
        try {
            socket.open();
            TProtocol protocol = new TBinaryProtocol(socket);
            SlaveEndPoints.Client client = new SlaveEndPoints.Client(protocol);
            mergeResponse = client.merge(filesToMerge);
        } catch (TTransportException e) {
            // This can happen if connection is refused or node is manually killed when sort is executing
            return new MergeResponse(Status.FAILED);
        }
        return mergeResponse;
    }
}
