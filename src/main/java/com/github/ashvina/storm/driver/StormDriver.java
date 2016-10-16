package com.github.ashvina.storm.driver;

import com.github.ashvina.storm.StormApplicationConfiguration;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

@Unit
public class StormDriver {
  static final Logger logger = Logger.getLogger(StormDriver.class.getName());

  private final REEFFileNames reefFileNames;
  private final EvaluatorRequestor requestor;
  private final int containerRam;
  private final int containerCores;
  private final int containerCount;

  AtomicInteger workerId = new AtomicInteger(0);

  @Inject
  public StormDriver(EvaluatorRequestor requestor,
                     @Parameter(StormApplicationConfiguration.ContainerRam.class) int ram,
                     @Parameter(StormApplicationConfiguration.ContainerCores.class) int cores,
                     @Parameter(StormApplicationConfiguration.ContainerCount.class) int num,
                     REEFFileNames reefFileNames) {
    this.requestor = requestor;
    this.reefFileNames = reefFileNames;
    this.containerRam = ram;
    this.containerCores = cores;
    this.containerCount = num;

    logger.info("Mem: " + containerRam);
    logger.info("Cores: " + containerCores);
    logger.info("Number: " + containerCount);
  }

  public class StormStartTime implements EventHandler<StartTime> {
    public void onNext(StartTime startTime) {
      EvaluatorRequest request = EvaluatorRequest
          .newBuilder()
          .setNumber(containerCount)
          .setMemory(containerRam)
          .setNumberOfCores(containerCores)
          .build();

      requestor.submit(request);
    }
  }

  public class StormAllocatedEvaluator implements EventHandler<AllocatedEvaluator> {
    public void onNext(AllocatedEvaluator evaluator) {
      String thisWorkerId = "worker-" + workerId.incrementAndGet();
      logger.info("Evaluator allocated: " + evaluator.getId());
      Configuration context = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, thisWorkerId)
          .build();
      logger.info("Submitting worker context: " + thisWorkerId);
      evaluator.submitContext(context);
    }
  }

  public class StormActiveContext implements EventHandler<ActiveContext> {
    public void onNext(ActiveContext activeContext) {
      final Configuration taskConf = TaskConfiguration.CONF
          .set(TaskConfiguration.TASK, StormWorkerTask.class)
          .set(TaskConfiguration.IDENTIFIER, "worker-" + activeContext.getId())
          .build();
      logger.info("Submitting worker task for newly activated context: " + activeContext.getId());
      activeContext.submitTask(taskConf);
    }
  }
}
