package de.claasklar.primitives.query;

import de.claasklar.primitives.CollectionName;

public final class Find extends Query {

  private final CollectionName collectionName;
  private final String queryName;
  private final FindOptions findOptions;

  public Find(CollectionName collectionName, String queryName, FindOptions findOptions) {
    this.collectionName = collectionName;
    this.queryName = queryName;
    this.findOptions = findOptions;
  }

  @Override
  public CollectionName getCollectionName() {
    return this.collectionName;
  }

  public FindOptions getFindOptions() {
    return this.findOptions;
  }

  @Override
  public String getQueryName() {
    return this.queryName;
  }
}
