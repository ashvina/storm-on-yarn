package com.github.ashvina.storm.driver;

import com.github.ashvina.storm.StormLauncher;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.io.*;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StormWorkerTask implements Task {

  static final Logger logger = Logger.getLogger(StormWorkerTask.class.getName());
  private final Path stormConfFile;
  Object lock = new Object();
  private Path stormPackagePath;

  @Inject
  public StormWorkerTask(final REEFFileNames fileNames) {
    stormPackagePath = Paths.get(fileNames.getGlobalFolder().getAbsolutePath(), StormLauncher.STORM_PACKAGE);
    stormConfFile = Paths.get(fileNames.getGlobalFolder().getAbsolutePath(), StormLauncher.STORM_YAML);
  }

  @Override
  public byte[] call(byte[] memento) throws Exception {
    executeCommand("tar -xvzf " + stormPackagePath);
    createConfFile();
    return new byte[0];
  }

  private void createConfFile() throws Exception {
    logger.info("Creating storm conf file for worker with random port");
    executeCommand(String.format("cp %s .", stormConfFile.toString()));
    new File(StormLauncher.STORM_YAML).setWritable(true, false);

    ServerSocket socket = new ServerSocket(0);
    int port = socket.getLocalPort();
    socket.close();

    ArrayList<String> configData = new ArrayList<>();
    configData.add("supervisor.slots.ports:");
    configData.add("    - " + port);

    try {
      BufferedWriter writer = new BufferedWriter(new FileWriter(StormLauncher.STORM_YAML, true));
      writer.newLine();
      for (String config : configData) {
        writer.write(config);
        writer.newLine();
      }
      writer.close();
      executeCommand("cat " + StormLauncher.STORM_YAML);

      String confDestination = "apache-storm-1.0.2/conf/" + StormLauncher.STORM_YAML;
      executeCommand(String.format("mv %s %s", StormLauncher.STORM_YAML, confDestination));
      executeCommand("bin/storm supervisor", new File("apache-storm-1.0.2"));
    } catch (Exception e) {
      logger.log(Level.WARNING, "", e);
    }

    synchronized (lock) {
      lock.wait();
    }
  }

  private void executeCommand(String cmd) throws Exception {
    executeCommand(cmd, new File("."));
  }

  private void executeCommand(String cmd, File workingDir) throws Exception {
    logger.info("Command: " + cmd);

    Process process = Runtime.getRuntime().exec(cmd, null, workingDir);
    process.waitFor();

    logStream(process.getErrorStream(), "Errout: ");
    logStream(process.getInputStream(), "Stdout: ");
  }

  private void logStream(InputStream processStream, String prefix) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(processStream));
    String line;
    while ((line = reader.readLine()) != null) {
      logger.info(prefix + line);
    }
  }
}
