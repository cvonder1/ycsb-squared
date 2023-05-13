package de.claasklar.random.distribution.document;

import de.claasklar.database.Database;
import de.claasklar.idStore.IdStore;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.IdLong;
import de.claasklar.primitives.span.Span;
import de.claasklar.random.distribution.id.IdDistribution;
import de.claasklar.specification.WriteSpecificationRegistry;

public class SimpleDocumentDistribution implements DocumentDistribution {

  private final CollectionName collectionName;
  private final IdDistribution idDistribution;
  private final IdStore idStore;
  private final Database database;
  private final WriteSpecificationRegistry registry;

  public SimpleDocumentDistribution(
      CollectionName collectionName,
      IdDistribution idDistribution,
      IdStore idStore,
      Database database,
      WriteSpecificationRegistry registry) {
    this.collectionName = collectionName;
    this.idDistribution = idDistribution;
    this.idStore = idStore;
    this.database = database;
    this.registry = registry;
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
      return new ReadDocumentRunnable(collectionName, new IdLong(nextId), span, database);
    } else {
      return new WriteDocumentRunnable(collectionName, new IdLong(nextId), span, registry);
    }
  }

  @Override
  public CollectionName getCollectionName() {
    return collectionName;
  }
}
