package de.claasklar.generation;

import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.query.Find;
import de.claasklar.primitives.query.FindOptions;
import de.claasklar.primitives.query.Query;

/**
 * A {@link QueryGenerator} implementation that always returns the same find query.
 */
public class SameFindGenerator implements QueryGenerator {

  private final CollectionName collectionName;
  private final FindOptions findOptions;

  public SameFindGenerator(CollectionName collectionName, FindOptions findOptions) {
    this.collectionName = collectionName;
    this.findOptions = findOptions;
  }

  @Override
  public Query generateQuery(String readSpecificationName) {
    return new Find(collectionName, readSpecificationName, findOptions);
  }

  @Override
  public CollectionName getCollectionName() {
    return collectionName;
  }
}
