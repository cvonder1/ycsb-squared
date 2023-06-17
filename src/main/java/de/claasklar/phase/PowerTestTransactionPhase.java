package de.claasklar.phase;

import de.claasklar.specification.TopSpecification;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PowerTestTransactionPhase implements TransactionPhase {
  private static final Logger logger =
      LoggerFactory.getLogger(WeightedRandomTransactionPhase.class);

  private final List<TopSpecification> specifications;
  private final Span applicationSpan;
  private final Tracer tracer;

  public PowerTestTransactionPhase(
      List<TopSpecification> specifications, Span applicationSpan, Tracer tracer) {
    this.specifications = specifications;
    this.applicationSpan = applicationSpan;
    this.tracer = tracer;
  }

  @Override
  public void run() {
    var transactionSpan =
        tracer
            .spanBuilder("transaction phase")
            .setParent(Context.current().with(applicationSpan))
            .startSpan();
    logger.atInfo().log("start of transaction phase");
    try {
      specifications.forEach(it -> it.runnable().run());
    } catch (Exception e) {
      logger.atError().log(e::getMessage);
      transactionSpan.recordException(e);
      transactionSpan.setStatus(StatusCode.ERROR);
    } finally {
      logger.atInfo().log("end of transaction phase");
      transactionSpan.end();
    }
  }
}
