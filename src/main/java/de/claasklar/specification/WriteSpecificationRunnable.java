package de.claasklar.specification;

import de.claasklar.database.Database;
import de.claasklar.generation.ContextlessDocumentGenerator;
import de.claasklar.idStore.IdStore;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.IdLong;
import de.claasklar.primitives.document.OurDocument;
import de.claasklar.util.TelemetryUtil;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import java.time.Clock;
import java.time.temporal.ChronoUnit;

public class WriteSpecificationRunnable implements Runnable {

  private final CollectionName collectionName;
  private final IdLong idLong;
  private final Span parentSpan;
  private final ContextlessDocumentGenerator generator;
  private final Database database;
  private final IdStore idStore;
  private final LongHistogram histogram;
  private final Attributes attributes;
  private final Tracer tracer;
  private final Clock clock;
  private OurDocument document;
  private boolean done = false;

  public WriteSpecificationRunnable(
      CollectionName collectionName,
      IdLong idLong,
      Span parentSpan,
      ContextlessDocumentGenerator generator,
      Database database,
      IdStore idStore,
      LongHistogram histogram,
      Attributes attributes,
      Tracer tracer,
      Clock clock) {
    this.collectionName = collectionName;
    this.idLong = idLong;
    this.parentSpan = parentSpan;
    this.generator = generator;
    this.database = database;
    this.idStore = idStore;
    this.histogram = histogram;
    this.attributes = attributes;
    this.tracer = tracer;
    this.clock = clock;
  }

  @Override
  public void run() {
    var start = clock.instant();
    var document = generator.generateDocument(idLong.toId());
    var runSpan = newSpan();
    try (var ignored = parentSpan.makeCurrent()) {
      this.document = database.write(collectionName, document, runSpan);
      this.done = true;
      this.idStore.store(collectionName, idLong);
      histogram.record(start.until(clock.instant(), ChronoUnit.MICROS), attributes);
    } catch (Exception e) {
      runSpan.setStatus(StatusCode.ERROR);
      runSpan.recordException(e);
      throw e;
    } finally {
      runSpan.end();
    }
  }

  public IdLong getIdLong() {
    if (!done) {
      throw new IllegalStateException("cannot access id before runnable is finished");
    }
    return this.idLong;
  }

  public OurDocument getDocument() {
    if (!done) {
      throw new IllegalStateException("cannot access document before runnable is finished");
    }
    return this.document;
  }

  private Span newSpan() {
    return tracer
        .spanBuilder("Write secondary document")
        .setParent(Context.current().with(parentSpan))
        .setAllAttributes(new TelemetryUtil().putId(attributes, idLong))
        .startSpan();
  }
}
