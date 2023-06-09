package de.claasklar.specification;

import de.claasklar.database.Database;
import de.claasklar.generation.DocumentGenerator;
import de.claasklar.idStore.IdStore;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.IdLong;
import de.claasklar.random.distribution.reference.ReferencesDistribution;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import java.time.Clock;
import java.util.concurrent.ExecutorService;

public final class WriteSpecification implements Specification {

  private final CollectionName collectionName;
  private final DocumentGenerator generator;
  private final ReferencesDistribution[] referencesDistributions;
  private final Database database;
  private final IdStore idStore;
  private final ExecutorService executor;
  private final LongHistogram histogram;
  private final Attributes attributes;
  private final Tracer tracer;
  private final Clock clock;

  public WriteSpecification(
      CollectionName collectionName,
      DocumentGenerator generator,
      ReferencesDistribution[] referencesDistributions,
      Database database,
      IdStore idStore,
      ExecutorService executor,
      LongHistogram histogram,
      Tracer tracer,
      Clock clock) {
    this.collectionName = collectionName;
    this.generator = generator;
    this.referencesDistributions = referencesDistributions;
    this.database = database;
    this.idStore = idStore;
    this.executor = executor;
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
        referencesDistributions,
        generator,
        database,
        idStore,
        executor,
        histogram,
        attributes,
        tracer,
        clock);
  }

  public CollectionName getCollectionName() {
    return this.collectionName;
  }
}
