package de.claasklar.specification;

import de.claasklar.generation.DocumentGenerator;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.IdLong;
import de.claasklar.primitives.document.OurDocument;
import de.claasklar.random.distribution.StdRandomNumberGenerator;
import de.claasklar.random.distribution.reference.ReferencesDistribution;
import de.claasklar.util.MapCollector;
import de.claasklar.util.Pair;
import de.claasklar.util.TelemetryUtil;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;

public class ComputeSpecificationRunnable implements DocumentGenerationSpecificationRunnable {

  private final CollectionName collectionName;
  private final IdLong idLong;
  private final Span parentSpan;
  private final ReferencesDistribution[] referencesDistributions;
  private final DocumentGenerator documentGenerator;
  private final ExecutorService executor;
  private final Tracer tracer;
  private final StdRandomNumberGenerator random = new StdRandomNumberGenerator();

  private OurDocument document;

  public ComputeSpecificationRunnable(
      CollectionName collectionName,
      IdLong idLong,
      Span parentSpan,
      ReferencesDistribution[] referencesDistributions,
      DocumentGenerator documentGenerator,
      ExecutorService executor,
      Tracer tracer) {
    this.collectionName = collectionName;
    this.idLong = idLong;
    this.parentSpan = parentSpan;
    this.referencesDistributions = referencesDistributions;
    this.documentGenerator = documentGenerator;
    this.executor = executor;
    this.tracer = tracer;
  }

  @Override
  public OurDocument getDocument() {
    if (document == null) {
      throw new IllegalStateException("cannot access document before runnable is finished");
    }
    return this.document;
  }

  @Override
  public void run() {
    random.setSeed(idLong.id());
    var runSpan = newSpan();
    try (var ignored = parentSpan.makeCurrent()) {
      var references =
          Arrays.stream(referencesDistributions)
              .parallel()
              .map(dist -> new Pair<>(dist.getCollectionName(), dist.next(runSpan)))
              .map(pair -> pair.mapSecond(runnable -> runnable.execute(executor)))
              .collect(new MapCollector<>());
      this.document = documentGenerator.generateDocument(idLong, references);
    } catch (Exception e) {
      runSpan.setStatus(StatusCode.ERROR);
      runSpan.recordException(e);
    } finally {
      runSpan.end();
      random.reset();
    }
  }

  private Span newSpan() {
    return tracer
        .spanBuilder("Write secondary document")
        .setParent(Context.current().with(parentSpan))
        .setAllAttributes(new TelemetryUtil().attributes(collectionName, idLong))
        .startSpan();
  }
}
