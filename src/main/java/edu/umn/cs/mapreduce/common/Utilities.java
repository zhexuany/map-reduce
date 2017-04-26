package edu.umn.cs.mapreduce.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;

/**
 * Created by zhexuany on 3/27/16.
 */

public class Utilities {
    private static final Logger LOG = LoggerFactory.getLogger(Utilities.class);

    public static int getRandomPort() throws IOException {
        ServerSocket socket = new ServerSocket(0);
        int randomPort = socket.getLocalPort();
        socket.close();
        return randomPort;
    }

    @SuppressWarnings({"ConstantConditions", "ResultOfMethodCallIgnored"})
    public static void deleteAllIntermediateFiles() {
        File intDir = new File(Constants.DEFAULT_INTERMEDIATE_DIR);
        if (intDir.exists()) {
            for (File file : intDir.listFiles()) {
                file.delete();
            }
            LOG.info("Removed all intermediate files...");
        } else {
            LOG.info("intermediate dir is not existed...");
        }
    }
}
