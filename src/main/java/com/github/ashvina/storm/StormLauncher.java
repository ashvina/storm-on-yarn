package com.github.ashvina.storm;

import com.github.ashvina.storm.driver.StormDriver;
import com.github.ashvina.storm.driver.StormWorkerTask;
import org.apache.commons.cli.*;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.REEF;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationModule;

import java.util.logging.Logger;

public class StormLauncher {
  public static final String STORM_PACKAGE = "apache-storm-1.0.2.tar.gz";
  public static final String STORM_YAML = "storm.yaml";

  private final Logger logger = Logger.getLogger(StormWorkerTask.class.getName());
  private ConfigurationModule stormConf = StormApplicationConfiguration.CONF;

  public static void main(String[] args) throws Exception {
    StormLauncher launcher = new StormLauncher();
    launcher.parseOptions(args);
    launcher.launchStormCluster(YarnClientConfiguration.CONF.build());
  }

  private void parseOptions(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(Option.builder("mem")
        .desc("container ram allocation mb")
        .hasArg()
        .build());

    options.addOption(Option.builder("cores")
        .desc("container cores allocation")
        .hasArg()
        .build());

    options.addOption(Option.builder("num")
        .desc("number of containers")
        .hasArg()
        .build());

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption("mem")) {
      int value = Integer.parseInt(cmd.getOptionValue("mem"));
      logger.info("Container memory mb: " + value);
      stormConf = stormConf.set(StormApplicationConfiguration.CONTAINER_RAM, value);
    }

    if (cmd.hasOption("cores")) {
      int value = Integer.parseInt(cmd.getOptionValue("cores"));
      logger.info("Container cores: " + value);
      stormConf = stormConf.set(StormApplicationConfiguration.CONTAINER_CORES, value);
    }

    if (cmd.hasOption("num")) {
      int value = Integer.parseInt(cmd.getOptionValue("num"));
      logger.info("Number of containers: " + value);
      stormConf = stormConf.set(StormApplicationConfiguration.CONTAINER_COUNT, value);
    }
  }

  private void launchStormCluster(Configuration runtimeConfig) throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(runtimeConfig);
    final REEF reef = injector.getInstance(REEF.class);
    reef.submit(getDriverConfig());
  }

  private Configuration getDriverConfig() {
    String fatJar = this.getClass().getProtectionDomain().getCodeSource().getLocation().getFile();

    Configuration basicDriverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.ON_DRIVER_STARTED, StormDriver.StormStartTime.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, StormDriver.StormAllocatedEvaluator.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, StormDriver.StormActiveContext.class)
        .set(DriverConfiguration.GLOBAL_LIBRARIES, fatJar)
        .set(DriverConfiguration.GLOBAL_FILES, STORM_PACKAGE)
        .set(DriverConfiguration.GLOBAL_FILES, STORM_YAML)
        .set(DriverConfiguration.DRIVER_MEMORY, 2048)
        .build();

    return Configurations.merge(stormConf.build(), basicDriverConf);
  }
}
