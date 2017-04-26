import edu.umn.cs.mapreduce.FileSplit;
import edu.umn.cs.mapreduce.master.MasterEndPointsImpl;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by zhexuany on 3/1/16.
 */

public class TestMaster {

    @Test
    public void testComputeSplitCount() throws IOException {
        String filename = "foo";
        File file = new File(filename);
        try {
            MasterEndPointsImpl masterEndPoints = new MasterEndPointsImpl(10, 2, 10000, 0, 0.0, 0.0);
            String input = "10 20 30 40 50 60 70 80 90 100";
            try (PrintWriter out = new PrintWriter(file)) {
                out.print(input);
            }

            List<FileSplit> fileSplits = masterEndPoints.computeSplits(filename, 10);
            assertEquals(3, fileSplits.size());
            assertEquals(true, fileSplits.toString().contains("offset:0, length:11"));
            assertEquals(true, fileSplits.toString().contains("offset:11, length:12"));
            assertEquals(true, fileSplits.toString().contains("offset:23, length:10"));

            fileSplits = masterEndPoints.computeSplits(filename, 30);
            assertEquals(1, fileSplits.size());

            fileSplits = masterEndPoints.computeSplits(filename, 3);
            assertEquals(10, fileSplits.size());
        } finally {
            if (!file.delete()) {
                System.out.println(file + "fails to delete");
            }
        }
    }

    @Test
    public void testComputeSplitOffsets() throws IOException {
        String filename = "testComputeSplits.txt";
        File file = new File(filename);
        try {
            MasterEndPointsImpl masterEndPoints = new MasterEndPointsImpl(10, 2, 10000, 0, 0.0, 0.0);
            String input = "10 20 30 40 50 60 70 80 90 100";
            try (PrintWriter out = new PrintWriter(file)) {
                out.print(input);
            }

            List<FileSplit> fileSplits = masterEndPoints.computeSplits(filename, 10);
            assertEquals(true, fileSplits.toString().contains("offset:0, length:11"));
            assertEquals(true, fileSplits.toString().contains("offset:11, length:12"));
            assertEquals(true, fileSplits.toString().contains("offset:23, length:10"));

            fileSplits = masterEndPoints.computeSplits(filename, 30);
            assertEquals(true, fileSplits.toString().contains("offset:0, length:30"));

            fileSplits = masterEndPoints.computeSplits(filename, 3);
            // seeking into file uses 0 based index, that's why 1st split is 5 bytes long
            assertEquals(true, fileSplits.toString().contains("offset:0, length:5"));
            assertEquals(true, fileSplits.toString().contains("offset:5, length:3"));
            assertEquals(true, fileSplits.toString().contains("offset:8, length:3"));
            assertEquals(true, fileSplits.toString().contains("offset:11, length:3"));
            assertEquals(true, fileSplits.toString().contains("offset:14, length:3"));
            assertEquals(true, fileSplits.toString().contains("offset:17, length:3"));
            assertEquals(true, fileSplits.toString().contains("offset:20, length:3"));
            assertEquals(true, fileSplits.toString().contains("offset:23, length:3"));
            assertEquals(true, fileSplits.toString().contains("offset:26, length:4"));
            assertEquals(true, fileSplits.toString().contains("offset:30, length:3"));
        } finally {
            if (!file.delete()) {
                System.out.println(file + "fails to delete");
            }
        }
    }


    @Test
    public void testMergeBatching() throws IOException {
        List<String> fakeFiles = new ArrayList<>();
        fakeFiles.add("file1");
        fakeFiles.add("file2");
        fakeFiles.add("file3");
        fakeFiles.add("file4");
        fakeFiles.add("file5");
        fakeFiles.add("file6");
        fakeFiles.add("file7");
        MasterEndPointsImpl masterEndPoints = new MasterEndPointsImpl(10, 2, 10000, 0, 0.0, 0.0);
        List<List<String>> mergedJobs = masterEndPoints.batchJobs(fakeFiles, 7);
        assertEquals(1, mergedJobs.size());
        assertEquals(fakeFiles, mergedJobs.get(0));

        mergedJobs = masterEndPoints.batchJobs(fakeFiles, 6);
        assertEquals(2, mergedJobs.size());
        assertEquals(fakeFiles.subList(0,6), mergedJobs.get(0));
        assertEquals(fakeFiles.subList(6, fakeFiles.size()), mergedJobs.get(1));

        mergedJobs = masterEndPoints.batchJobs(fakeFiles, 5);
        assertEquals(2, mergedJobs.size());
        assertEquals(fakeFiles.subList(0,5), mergedJobs.get(0));
        assertEquals(fakeFiles.subList(5, fakeFiles.size()), mergedJobs.get(1));

        mergedJobs = masterEndPoints.batchJobs(fakeFiles, 4);
        assertEquals(2, mergedJobs.size());
        assertEquals(fakeFiles.subList(0,4), mergedJobs.get(0));
        assertEquals(fakeFiles.subList(4, fakeFiles.size()), mergedJobs.get(1));

        mergedJobs = masterEndPoints.batchJobs(fakeFiles, 3);
        assertEquals(3, mergedJobs.size());
        assertEquals(fakeFiles.subList(0,3), mergedJobs.get(0));
        assertEquals(fakeFiles.subList(3,6), mergedJobs.get(1));
        assertEquals(fakeFiles.subList(6, fakeFiles.size()), mergedJobs.get(2));

        mergedJobs = masterEndPoints.batchJobs(fakeFiles, 2);
        assertEquals(4, mergedJobs.size());
        assertEquals(fakeFiles.subList(0,2), mergedJobs.get(0));
        assertEquals(fakeFiles.subList(2,4), mergedJobs.get(1));
        assertEquals(fakeFiles.subList(4,6), mergedJobs.get(2));
        assertEquals(fakeFiles.subList(6, fakeFiles.size()), mergedJobs.get(3));

        mergedJobs = masterEndPoints.batchJobs(fakeFiles, 1);
        assertEquals(7, mergedJobs.size());
        assertEquals(fakeFiles.subList(0,1), mergedJobs.get(0));
        assertEquals(fakeFiles.subList(1,2), mergedJobs.get(1));
        assertEquals(fakeFiles.subList(2,3), mergedJobs.get(2));
        assertEquals(fakeFiles.subList(3,4), mergedJobs.get(3));
        assertEquals(fakeFiles.subList(4,5), mergedJobs.get(4));
        assertEquals(fakeFiles.subList(5,6), mergedJobs.get(5));
        assertEquals(fakeFiles.subList(6, fakeFiles.size()), mergedJobs.get(6));
    }
}
