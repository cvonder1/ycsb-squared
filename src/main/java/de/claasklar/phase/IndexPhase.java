package de.claasklar.phase;

import de.claasklar.database.Database;
import de.claasklar.primitives.index.IndexConfiguration;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexPhase {
  private static final Logger logger = LoggerFactory.getLogger(IndexPhase.class);
  private final IndexConfiguration[] indexConfigurations;
  private final Database database;
  private final Span span;
  private final Tracer tracer;

  public IndexPhase(
      IndexConfiguration[] indexConfigurations, Database database, Span span, Tracer tracer) {
    this.indexConfigurations = indexConfigurations;
    this.database = database;
    this.span = span;
    this.tracer = tracer;
  }

  public void createIndexes() {
    var indexSpan =
        tracer.spanBuilder("index phase").setParent(Context.current().with(span)).startSpan();
    logger.atInfo().log("beginning of index phase");
    try {
      Arrays.stream(indexConfigurations).forEach(it -> database.createIndex(it, indexSpan));
    } catch (Exception e) {
      indexSpan.recordException(e);
      indexSpan.setStatus(StatusCode.ERROR);
      throw new IndexCreationFailure(e);
    } finally {
      logger.atInfo().log("end of index phase");
      indexSpan.end();
    }
  }
}
