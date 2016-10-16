package com.github.ashvina.storm;

import com.github.ashvina.storm.driver.StormDriver;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.REEF;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

public class StormLauncher {
  public static final String STORM_PACKAGE = "apache-storm-1.0.2.tar.gz";
  public static final String STORM_YAML = "storm.yaml";

  public static void main(String[] args) throws InjectionException {
    StormLauncher launcher = new StormLauncher();
    launcher.launchStormCluster(YarnClientConfiguration.CONF.build());
  }

  private void launchStormCluster(Configuration runtimeConfig) throws InjectionException {
    Configuration reefDriverConf = getDriverConfig();

    final Injector injector = Tang.Factory.getTang().newInjector(runtimeConfig);
    final REEF reef = injector.getInstance(REEF.class);
    reef.submit(reefDriverConf);
  }

  Configuration getDriverConfig() {
    String fatjar = this.getClass().getProtectionDomain().getCodeSource().getLocation().getFile();

    Configuration basicDriverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.ON_DRIVER_STARTED, StormDriver.StormStartTime.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, StormDriver.StormAllocatedEvaluator.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, StormDriver.StormActiveContext.class)
        .set(DriverConfiguration.GLOBAL_LIBRARIES, fatjar)
        .set(DriverConfiguration.GLOBAL_FILES, STORM_PACKAGE)
        .set(DriverConfiguration.GLOBAL_FILES, STORM_YAML)
        .set(DriverConfiguration.DRIVER_MEMORY, 2048)
        .build();

    return basicDriverConf;
  }
}
