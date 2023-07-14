package de.claasklar.random.distribution.document;

import de.claasklar.database.Database;
import de.claasklar.idStore.IdStore;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.IdLong;
import de.claasklar.random.distribution.DistributionProperties;
import de.claasklar.random.distribution.id.IdDistribution;
import de.claasklar.specification.DocumentGenerationSpecificationRegistry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import java.util.List;

public class SimpleDocumentDistribution implements DocumentDistribution {

  private final CollectionName collectionName;
  private final IdDistribution idDistribution;
  private final IdStore idStore;
  private final Database database;
  private final DocumentGenerationSpecificationRegistry registry;
  private final Tracer tracer;

  public SimpleDocumentDistribution(
      CollectionName collectionName,
      IdDistribution idDistribution,
      IdStore idStore,
      Database database,
      DocumentGenerationSpecificationRegistry registry,
      Tracer tracer) {
    this.collectionName = collectionName;
    this.idDistribution = idDistribution;
    this.idStore = idStore;
    this.database = database;
    this.registry = registry;
    this.tracer = tracer;
  }

  /**
   * Will enter span for both read and write futures
   *
   * @param span
   * @return DocumentFuture
   */
  @Override
  public DocumentRunnable next(Span span) {
    var nextId = idDistribution.nextAsLong();
    if (idStore.exists(collectionName, nextId)) {
      return new ReadDocumentRunnable(collectionName, new IdLong(nextId), span, database, tracer);
    } else {
      return new WriteDocumentRunnable(collectionName, new IdLong(nextId), span, registry);
    }
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
