import edu.umn.cs.mapreduce.FileSplit;
import edu.umn.cs.mapreduce.MergeResponse;
import edu.umn.cs.mapreduce.SortResponse;
import edu.umn.cs.mapreduce.Status;
import edu.umn.cs.mapreduce.common.Utilities;
import edu.umn.cs.mapreduce.common.Constants;
import edu.umn.cs.mapreduce.master.MasterEndPointsImpl;
import edu.umn.cs.mapreduce.slave.SlaveEndPointsImpl;
import org.apache.thrift.TException;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by zhexuany on 3/1/16.
 */
public class TestSlave {

    @Test
    public void testSort() throws IOException, TException {
        String filename = "foo";
        File file = new File(filename);
        try {
            MasterEndPointsImpl masterEndPoints = new MasterEndPointsImpl(10, 2, 10000, 0, 0.0, 0.0);
            String input = "90 60 80 70 100 40 10 50 20 30";
            try (PrintWriter out = new PrintWriter(file)) {
                out.print(input);
            }

            File intDir = new File(Constants.DEFAULT_INTERMEDIATE_DIR);
            if (!intDir.exists()) {
                intDir.mkdir();
            }

            List<FileSplit> fileSplits = masterEndPoints.computeSplits(filename, 15);
            assertEquals(2, fileSplits.size());
            assertEquals(true, fileSplits.toString().contains("offset:0, length:15"));
            assertEquals(true, fileSplits.toString().contains("offset:15, length:15"));

            SlaveEndPointsImpl slaveEndPoints = new SlaveEndPointsImpl("localhost", "9191");
            int i = 0;
            for (FileSplit fileSplit : fileSplits) {
                i++;
                SortResponse sortResponse = slaveEndPoints.sort(fileSplit);
                assertEquals(Status.SUCCESS, sortResponse.getStatus());
                assertEquals(true, sortResponse.getIntermediateFilePath().contains("file_localhost_9191_"));
                String sortedStr = readFile(sortResponse.getIntermediateFilePath());
                if (i == 1) {
                    assertEquals("60 70 80 90 100", sortedStr);
                } else if (i == 2) {
                    assertEquals("10 20 30 40 50", sortedStr);
                }
            }
        } finally {
            Utilities.deleteAllIntermediateFiles();
            if(!file.delete()) {
                System.out.println(file + "fails to be deleted");
            }
        }
    }

    @Test
    public void testMerge() throws IOException, TException {
        String filename = "foo";
        File file = new File(filename);
        File intDir = new File(Constants.DEFAULT_INTERMEDIATE_DIR);
        if (!intDir.exists()) {
            intDir.mkdir();
        }
        try {
            MasterEndPointsImpl masterEndPoints = new MasterEndPointsImpl(10, 2, 10000, 0, 0.0, 0.0);
            String input = "90 60 80 70 100 40 10 50 20 30";
            try (PrintWriter out = new PrintWriter(file)) {
                out.print(input);
            }

            List<FileSplit> fileSplits = masterEndPoints.computeSplits(filename, 15);
            assertEquals(2, fileSplits.size());
            assertEquals(true, fileSplits.toString().contains("offset:0, length:15"));
            assertEquals(true, fileSplits.toString().contains("offset:15, length:15"));

            SlaveEndPointsImpl slaveEndPoints = new SlaveEndPointsImpl("localhost", "9191");
            int i = 0;
            List<String> intFiles = new ArrayList<>();
            for (FileSplit fileSplit : fileSplits) {
                i++;
                SortResponse sortResponse = slaveEndPoints.sort(fileSplit);
                assertEquals(Status.SUCCESS, sortResponse.getStatus());
                assertEquals(true, sortResponse.getIntermediateFilePath().contains("file_localhost_9191_"));
                String sortedStr = readFile(sortResponse.getIntermediateFilePath());
                intFiles.add(sortResponse.getIntermediateFilePath());
                if (i == 1) {
                    assertEquals("60 70 80 90 100", sortedStr);
                } else if (i == 2) {
                    assertEquals("10 20 30 40 50", sortedStr);
                }
            }

            MergeResponse mergeResponse = slaveEndPoints.merge(intFiles);
            assertEquals(Status.SUCCESS, mergeResponse.getStatus());
            assertEquals(true, mergeResponse.getIntermediateFilePath().contains("file_localhost_9191_"));
            String mergedStr = readFile(mergeResponse.getIntermediateFilePath());
            assertEquals("10 20 30 40 50 60 70 80 90 100", mergedStr);
        } finally {
            Utilities.deleteAllIntermediateFiles();
            if(!file.delete()) {
                System.out.println(file + "fails to be deleted");
            }
        }
    }

    private String readFile(String path) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded);
    }
}
