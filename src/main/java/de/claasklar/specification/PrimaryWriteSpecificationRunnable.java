package de.claasklar.specification;

import de.claasklar.database.Database;
import de.claasklar.generation.DocumentGenerator;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.Document;
import de.claasklar.primitives.document.IdLong;
import de.claasklar.random.distribution.reference.ReferencesDistribution;
import de.claasklar.util.MapCollector;
import de.claasklar.util.Pair;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;

public class PrimaryWriteSpecificationRunnable implements Runnable {

  private final CollectionName collectionName;
  private final IdLong id;
  private final ReferencesDistribution[] referencesDistributions;
  private final DocumentGenerator generator;
  private final Database database;
  private final ExecutorService executor;
  private final LongHistogram histogram;
  private final Attributes attributes;
  private final Tracer tracer;
  private final Clock clock;
  private boolean wasRun = false;
  private Document document;

  public PrimaryWriteSpecificationRunnable(
      CollectionName collectionName,
      IdLong id,
      ReferencesDistribution[] referencesDistributions,
      DocumentGenerator generator,
      Database database,
      ExecutorService executor,
      LongHistogram histogram,
      Attributes attributes,
      Tracer tracer,
      Clock clock) {
    this.collectionName = collectionName;
    this.id = id;
    this.referencesDistributions = referencesDistributions;
    this.generator = generator;
    this.database = database;
    this.executor = executor;
    this.histogram = histogram;
    this.attributes = attributes;
    this.tracer = tracer;
    this.clock = clock;
  }

  @Override
  public void run() {
    var start = clock.instant();
    var span = newSpan();
    try (var ignored = span.makeCurrent()) {
      var references =
          Arrays.stream(referencesDistributions)
              .parallel()
              .map(dist -> new Pair<>(dist.getCollectionName(), dist.next(span)))
              .map(pair -> pair.mapSecond(runnable -> runnable.execute(executor)))
              .collect(new MapCollector<>());
      var document = generator.generateDocument(id.toId(), references);
      database.write(collectionName, document, span);
      this.document = document;
      this.wasRun = true;
      histogram.record(start.until(clock.instant(), ChronoUnit.MILLIS), attributes);
    } catch (Exception e) {
      span.setStatus(StatusCode.ERROR, "Could not create primary document with the id " + id);
      span.recordException(e);
    } finally {
      span.end();
    }
  }

  public boolean wasRun() {
    return this.wasRun;
  }

  public Document getDocument() {
    if (!wasRun) {
      throw new IllegalStateException("cannot access document before runnable was run");
    }
    return this.document;
  }

  private Span newSpan() {
    return tracer
        .spanBuilder("Writing primary document")
        .setAllAttributes(
            attributes.toBuilder()
                .put("id_long", id.toString())
                .put("id", id.toId().toString())
                .build())
        .setNoParent()
        .startSpan();
  }
}
