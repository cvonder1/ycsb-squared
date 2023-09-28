package de.claasklar.specification;

import de.claasklar.database.Database;
import de.claasklar.generation.DocumentGenerator;
import de.claasklar.idStore.IdStore;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.IdLong;
import de.claasklar.primitives.document.OurDocument;
import de.claasklar.random.distribution.reference.ReferencesDistribution;
import de.claasklar.util.MapCollector;
import de.claasklar.util.Pair;
import de.claasklar.util.TelemetryConfig;
import de.claasklar.util.TelemetryUtil;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import java.time.Clock;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public class WriteSpecificationRunnable implements DocumentGenerationSpecificationRunnable {

  private final CollectionName collectionName;
  private final IdLong idLong;
  private final Span parentSpan;
  private final ReferencesDistribution[] referencesDistributions;
  private final DocumentGenerator generator;
  private final Database database;
  private final IdStore idStore;
  private final ExecutorService executor;
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
      ReferencesDistribution[] referencesDistributions,
      DocumentGenerator generator,
      Database database,
      IdStore idStore,
      ExecutorService executor,
      LongHistogram histogram,
      Attributes attributes,
      Tracer tracer,
      Clock clock) {
    this.collectionName = collectionName;
    this.idLong = idLong;
    this.parentSpan = parentSpan;
    this.referencesDistributions = referencesDistributions;
    this.generator = generator;
    this.database = database;
    this.idStore = idStore;
    this.executor = executor;
    this.histogram = histogram;
    this.attributes = attributes;
    this.tracer = tracer;
    this.clock = clock;
  }

  @Override
  public void run() {
    var start = clock.instant();
    var runSpan = newSpan();
    try (var ignored = parentSpan.makeCurrent()) {
      var referencesRunnables =
          Arrays.stream(referencesDistributions)
              .map(dist -> new Pair<>(dist.getCollectionName(), dist.next(runSpan)))
              .collect(new MapCollector<>());
      var futures =
          referencesRunnables.values().stream()
              .map(it -> CompletableFuture.runAsync(it, executor))
              .toArray(CompletableFuture[]::new);
      CompletableFuture.allOf(futures).join();
      var references =
          referencesRunnables.entrySet().stream()
              .map(entry -> new Pair<>(entry.getKey(), entry.getValue().getDocuments()))
              .collect(new MapCollector<>());
      var document = generator.generateDocument(idLong, references);
      this.document = database.write(collectionName, document, runSpan);
      this.done = true;
      this.idStore.store(collectionName, idLong);
      histogram.record(
          start.until(clock.instant(), TelemetryConfig.DURATION_RESOLUTION), attributes);
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

  @Override
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
