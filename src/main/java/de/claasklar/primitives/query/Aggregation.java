package de.claasklar.primitives.query;

import de.claasklar.primitives.CollectionName;

public final class Aggregation extends Query {

  private final String queryName;
  private final AggregationOptions aggregationOptions;

  public Aggregation(String queryName, AggregationOptions aggregationOptions) {
    this.queryName = queryName;
    this.aggregationOptions = aggregationOptions;
  }

  @Override
  public CollectionName getCollectionName() {
    return new CollectionName(aggregationOptions.getAggregate());
  }

  @Override
  public String getQueryName() {
    return queryName;
  }

  public AggregationOptions getAggregationOptions() {
    return aggregationOptions;
  }
}
