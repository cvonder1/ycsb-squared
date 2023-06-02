package de.claasklar.specification;

import static io.opentelemetry.api.common.AttributeKey.stringKey;

import de.claasklar.database.Database;
import de.claasklar.generation.ContextDocumentGenerator;
import de.claasklar.idStore.IdStore;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.IdLong;
import de.claasklar.random.distribution.reference.ReferencesDistribution;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.trace.Tracer;
import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class PrimaryWriteSpecification implements Specification {

  private final CollectionName collectionName;
  private final ReferencesDistribution[] referencesDistributions;
  private final ContextDocumentGenerator generator;
  private final Database database;
  private final ExecutorService executor;
  private final LongHistogram histogram;
  private final IdStore idStore;
  private final Attributes attributes;
  private final Tracer tracer;
  private final Clock clock;
  private final AtomicLong currentId;

  public PrimaryWriteSpecification(
      CollectionName collectionName,
      ReferencesDistribution[] referencesDistributions,
      ContextDocumentGenerator generator,
      Database database,
      ExecutorService executor,
      LongHistogram histogram,
      IdStore idStore,
      Tracer tracer,
      Clock clock) {
    this.collectionName = collectionName;
    this.referencesDistributions = referencesDistributions;
    this.generator = generator;
    this.database = database;
    this.executor = executor;
    this.histogram = histogram;
    this.idStore = idStore;
    this.attributes =
        Attributes.of(
            stringKey("collection"), collectionName.toString(), stringKey("operation"), "WRITE");
    this.tracer = tracer;
    this.clock = clock;
    this.currentId = new AtomicLong(1);
  }

  public PrimaryWriteSpecificationRunnable runnable() {
    return new PrimaryWriteSpecificationRunnable(
        collectionName,
        new IdLong(currentId.getAndIncrement()),
        referencesDistributions,
        generator,
        database,
        executor,
        histogram,
        attributes,
        idStore,
        tracer,
        clock);
  }
}
