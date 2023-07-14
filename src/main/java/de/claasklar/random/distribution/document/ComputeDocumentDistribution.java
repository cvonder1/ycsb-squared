package de.claasklar.random.distribution.document;

import de.claasklar.primitives.CollectionName;
import de.claasklar.random.distribution.DistributionProperties;
import de.claasklar.random.distribution.id.IdDistribution;
import de.claasklar.specification.DocumentGenerationSpecificationRegistry;
import io.opentelemetry.api.trace.Span;
import java.util.List;

public class ComputeDocumentDistribution implements DocumentDistribution {

  private final CollectionName collectionName;
  private final IdDistribution idDistribution;
  private final DocumentGenerationSpecificationRegistry documentGenerationSpecificationRegistry;

  public ComputeDocumentDistribution(
      CollectionName collectionName,
      IdDistribution idDistribution,
      DocumentGenerationSpecificationRegistry documentGenerationSpecificationRegistry) {
    if (!idDistribution.distributionProperties().contains(DistributionProperties.REPEATABLE)) {
      throw new IllegalArgumentException(
          "not repeatable IdDistribution for collection " + collectionName);
    }
    this.collectionName = collectionName;
    this.idDistribution = idDistribution;
    this.documentGenerationSpecificationRegistry = documentGenerationSpecificationRegistry;
  }

  @Override
  public DocumentRunnable next(Span span) {
    var nextId = idDistribution.next();
    return new DocumentGenerationRunnable(
        collectionName, nextId, span, documentGenerationSpecificationRegistry);
  }

  @Override
  public CollectionName getCollectionName() {
    return collectionName;
  }

  @Override
  public List<DistributionProperties> distributionProperties() {
    return DistributionProperties.and(
        List.of(DistributionProperties.REPEATABLE), idDistribution.distributionProperties());
  }
}
