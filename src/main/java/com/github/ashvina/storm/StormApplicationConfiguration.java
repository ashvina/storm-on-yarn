package com.github.ashvina.storm;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredParameter;

public class StormApplicationConfiguration extends ConfigurationModuleBuilder {
  public static final RequiredParameter<Integer> CONTAINER_RAM = new RequiredParameter<>();
  public static final RequiredParameter<Integer> CONTAINER_CORES = new RequiredParameter<>();
  public static final RequiredParameter<Integer> CONTAINER_COUNT = new RequiredParameter<>();

  public static final ConfigurationModule CONF = new StormApplicationConfiguration()
      .bindNamedParameter(ContainerRam.class, CONTAINER_RAM)
      .bindNamedParameter(ContainerCores.class, CONTAINER_CORES)
      .bindNamedParameter(ContainerCount.class, CONTAINER_COUNT)
      .build();

  @NamedParameter
  public class ContainerRam implements Name<Integer> {
  }

  @NamedParameter
  public class ContainerCores implements Name<Integer> {
  }

  @NamedParameter
  public class ContainerCount implements Name<Integer> {
  }
}
