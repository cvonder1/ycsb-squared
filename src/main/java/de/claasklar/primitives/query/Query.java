package de.claasklar.primitives.query;

import de.claasklar.primitives.CollectionName;

public abstract sealed class Query permits Aggregation, Find {
  public abstract CollectionName getCollectionName();

  /**
   * This is the query's name used to track its performance across different transactions. Should be
   * constant for different instances of the same query.
   *
   * @return Name of the query.
   */
  public abstract String getQueryName();
}
