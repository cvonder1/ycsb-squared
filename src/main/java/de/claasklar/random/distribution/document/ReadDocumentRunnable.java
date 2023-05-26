package de.claasklar.random.distribution.document;

import de.claasklar.database.Database;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.IdLong;
import de.claasklar.primitives.document.OurDocument;
import de.claasklar.util.TelemetryUtil;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

/** For existing documents */
public final class ReadDocumentRunnable implements DocumentRunnable {

  private final CollectionName collectionName;
  private final IdLong id;
  private final Span parentSpan;
  private final Database database;
  private final Tracer tracer;
  private boolean wasRun = false;
  private OurDocument document;

  public ReadDocumentRunnable(
      CollectionName collectionName, IdLong id, Span parentSpan, Database database, Tracer tracer) {
    this.collectionName = collectionName;
    this.id = id;
    this.parentSpan = parentSpan;
    this.database = database;
    this.tracer = tracer;
  }

  @Override
  public OurDocument getDocument() {
    if (!wasRun) {
      throw new IllegalStateException("ReadDocumentRunnable must first be run");
    }
    return this.document;
  }

  @Override
  public boolean wasRun() {
    return this.wasRun;
  }

  @Override
  public void run() {
    if (this.wasRun) {
      throw new IllegalStateException("ReadDocumentRunnable can only be executed once");
    }
    var runSpan = newSpan();
    try (var ignored = runSpan.makeCurrent()) {
      this.document =
          database
              .read(this.collectionName, this.id.toId(), runSpan)
              .orElseThrow(() -> new NoSuchDocumentException(this.collectionName, this.id.toId()));
      this.wasRun = true;
    } catch (Exception e) {
      runSpan.setStatus(StatusCode.ERROR);
      runSpan.recordException(e);
      throw e;
    } finally {
      runSpan.end();
    }
  }

  public IdLong getId() {
    return id;
  }

  private Span newSpan() {
    return tracer
        .spanBuilder("read existing document")
        .setParent(Context.current().with(parentSpan))
        .setAllAttributes(new TelemetryUtil().attributes(collectionName, id))
        .startSpan();
  }
}
