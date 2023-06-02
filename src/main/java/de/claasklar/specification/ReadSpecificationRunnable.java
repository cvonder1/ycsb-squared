package de.claasklar.specification;

import de.claasklar.database.Database;
import de.claasklar.generation.QueryGenerator;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import java.time.Clock;
import java.time.temporal.ChronoUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadSpecificationRunnable implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(ReadSpecificationRunnable.class);

  private final QueryGenerator queryGenerator;
  private final String readSpecificationName;
  private final Database database;
  private final Attributes attributes;
  private final Tracer tracer;
  private final Clock clock;
  private final LongHistogram histogram;

  public ReadSpecificationRunnable(
      QueryGenerator queryGenerator,
      String readSpecificationName,
      Database database,
      Attributes attributes,
      Tracer tracer,
      Clock clock,
      LongHistogram histogram) {
    this.queryGenerator = queryGenerator;
    this.readSpecificationName = readSpecificationName;
    this.database = database;
    this.attributes = attributes;
    this.tracer = tracer;
    this.clock = clock;
    this.histogram = histogram;
  }

  @Override
  public void run() {
    var span = newSpan();
    try {
      var query = queryGenerator.generateQuery(readSpecificationName);
      var start = clock.instant();
      database.executeQuery(query, span);
      histogram.record(start.until(clock.instant(), ChronoUnit.MICROS), attributes);
    } catch (Exception e) {
      span.setStatus(StatusCode.ERROR);
      span.recordException(e);
      logger.atError().log("failed to run " + readSpecificationName + ": " + e.getMessage());
    } finally {
      span.end();
    }
  }

  private Span newSpan() {
    return tracer
        .spanBuilder("run query " + readSpecificationName)
        .setAllAttributes(attributes)
        .setNoParent()
        .startSpan();
  }
}
