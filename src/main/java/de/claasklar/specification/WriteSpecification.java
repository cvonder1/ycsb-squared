package de.claasklar.specification;

import de.claasklar.database.Database;
import de.claasklar.generation.ContextlessDocumentGenerator;
import de.claasklar.idStore.IdStore;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.IdLong;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import java.time.Clock;

public final class WriteSpecification implements Specification {

  private final CollectionName collectionName;
  private final ContextlessDocumentGenerator generator;
  private final Database database;
  private final IdStore idStore;
  private final LongHistogram histogram;
  private final Attributes attributes;
  private final Tracer tracer;
  private final Clock clock;

  public WriteSpecification(
      CollectionName collectionName,
      ContextlessDocumentGenerator generator,
      Database database,
      IdStore idStore,
      LongHistogram histogram,
      Tracer tracer,
      Clock clock) {
    this.collectionName = collectionName;
    this.generator = generator;
    this.database = database;
    this.idStore = idStore;
    this.histogram = histogram;
    this.attributes =
        Attributes.of(
            AttributeKey.stringKey("collection"),
            collectionName.toString(),
            AttributeKey.stringKey("operation"),
            "WRITE");
    this.tracer = tracer;
    this.clock = clock;
  }

  public WriteSpecificationRunnable runnable(IdLong id, Span span) {
    return new WriteSpecificationRunnable(
        collectionName,
        id,
        span,
        generator,
        database,
        idStore,
        histogram,
        attributes,
        tracer,
        clock);
  }

  public CollectionName getCollectionName() {
    return this.collectionName;
  }
}
