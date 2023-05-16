package de.claasklar.specification;

import de.claasklar.database.Database;
import de.claasklar.generation.ContextlessDocumentGenerator;
import de.claasklar.idStore.IdStore;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.IdLong;
import de.claasklar.primitives.span.Span;

public class WriteSpecification implements Specification {

  private final CollectionName collectionName;
  private final ContextlessDocumentGenerator generator;
  private final Database database;
  private final IdStore idStore;

  public WriteSpecification(
      CollectionName collectionName,
      ContextlessDocumentGenerator generator,
      Database database,
      IdStore idStore) {
    this.collectionName = collectionName;
    this.generator = generator;
    this.database = database;
    this.idStore = idStore;
  }

  public WriteSpecificationRunnable runnable(IdLong id, Span span) {
    return new WriteSpecificationRunnable(collectionName, id, span, generator, database, idStore);
  }

  public CollectionName getCollectionName() {
    return this.collectionName;
  }
}
