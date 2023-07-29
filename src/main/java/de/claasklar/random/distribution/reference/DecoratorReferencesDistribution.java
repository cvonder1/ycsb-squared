package de.claasklar.random.distribution.reference;

import de.claasklar.primitives.CollectionName;
import de.claasklar.random.distribution.Distribution;
import de.claasklar.random.distribution.DistributionProperties;
import de.claasklar.random.distribution.document.DocumentDistribution;
import de.claasklar.random.distribution.document.DocumentRunnable;
import io.opentelemetry.api.trace.Span;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class DecoratorReferencesDistribution implements ReferencesDistribution {

  private final Distribution<Long> countDistribution;
  private final DocumentDistribution documentDistribution;
  private final ExecutorService executorService;

  public DecoratorReferencesDistribution(
      Distribution<Long> countDistribution,
      DocumentDistribution documentDistribution,
      ExecutorService executorService) {
    this.countDistribution = countDistribution;
    this.documentDistribution = documentDistribution;
    this.executorService = executorService;
  }

  @Override
  public ReferencesRunnable next(Span span) {
    var numReferencedDocuments = countDistribution.sample();
    if (numReferencedDocuments < 0) {
      throw new IllegalArgumentException("cannot reference less than zero documents");
    }
    if (numReferencedDocuments > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "cannot reference more than " + Integer.MAX_VALUE + " documents");
    }
    var documentRunnables = new DocumentRunnable[(int) (long) numReferencedDocuments];
    for (int i = 0; i < numReferencedDocuments; i++) {
      documentRunnables[i] = documentDistribution.next(span);
    }
    return new ReferencesRunnable(documentRunnables, executorService);
  }

  @Override
  public CollectionName getCollectionName() {
    return documentDistribution.getCollectionName();
  }

  @Override
  public List<DistributionProperties> distributionProperties() {
    return DistributionProperties.and(
        countDistribution.distributionProperties(), documentDistribution.distributionProperties());
  }
}
