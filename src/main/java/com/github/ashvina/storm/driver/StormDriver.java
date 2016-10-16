package com.github.ashvina.storm.driver;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.logging.Logger;

@Unit
public class StormDriver {
  static final Logger logger = Logger.getLogger(StormDriver.class.getName());
  EvaluatorRequestor requestor;
  REEFFileNames reefFileNames;

  // count of workers
  // size of workers

  @Inject
  public StormDriver(EvaluatorRequestor requestor, REEFFileNames reefFileNames) {
    this.requestor = requestor;
    this.reefFileNames = reefFileNames;
  }

  public class StormStartTime implements EventHandler<StartTime> {
    public void onNext(StartTime startTime) {
      EvaluatorRequest request = EvaluatorRequest
          .newBuilder()
          .setNumber(1)
          .setMemory(2048)
          .setNumberOfCores(2)
          .build();

      requestor.submit(request);
    }
  }

  public class StormAllocatedEvaluator implements EventHandler<AllocatedEvaluator> {
    public void onNext(AllocatedEvaluator evaluator) {
      logger.info("Evaluator allocated: " + evaluator.getId());
      // TODO id of worker
      Configuration context = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, "worker")
          .build();
      logger.info("Submitting worker context: " + evaluator.getId());
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
