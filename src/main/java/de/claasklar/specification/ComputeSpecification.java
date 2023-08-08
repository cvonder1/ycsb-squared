package de.claasklar.specification;

import de.claasklar.generation.DocumentGenerator;
import de.claasklar.idStore.IdStore;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.IdLong;
import de.claasklar.random.distribution.DistributionProperties;
import de.claasklar.random.distribution.reference.ReferencesDistribution;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import java.util.concurrent.ExecutorService;

public class ComputeSpecification implements DocumentGenerationSpecification {
  private final CollectionName collectionName;
  private final DocumentGenerator generator;
  private final ReferencesDistribution[] referencesDistributions;
  private final IdStore idStore;
  private final ExecutorService executor;
  private final Tracer tracer;

  public ComputeSpecification(
      CollectionName collectionName,
      DocumentGenerator generator,
      ReferencesDistribution[] referencesDistributions,
      IdStore idStore,
      ExecutorService executor,
      Tracer tracer) {
    for (var referencesDistribution : referencesDistributions) {
      if (!referencesDistribution
          .distributionProperties()
          .contains(DistributionProperties.REPEATABLE)) {
        throw new IllegalArgumentException(
            "not repeatable ReferencesDistribution for collection "
                + referencesDistribution.getCollectionName()
                + " is not acceptable");
      }
    }
    this.collectionName = collectionName;
    this.generator = generator;
    this.referencesDistributions = referencesDistributions;
    this.idStore = idStore;
    this.executor = executor;
    this.tracer = tracer;
  }

  @Override
  public DocumentGenerationSpecificationRunnable runnable(IdLong id, Span span) {
    return new ComputeSpecificationRunnable(
        collectionName, id, span, referencesDistributions, generator, idStore, executor, tracer);
  }

  @Override
  public CollectionName getCollectionName() {
    return collectionName;
  }
}
