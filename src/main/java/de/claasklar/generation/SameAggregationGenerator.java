package de.claasklar.generation;

import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.query.Aggregation;
import de.claasklar.primitives.query.AggregationOptions;
import de.claasklar.primitives.query.Query;

public class SameAggregationGenerator implements QueryGenerator {

  private final CollectionName collectionName;
  private final AggregationOptions aggregationOptions;

  public SameAggregationGenerator(
      CollectionName collectionName, AggregationOptions aggregationOptions) {
    this.collectionName = collectionName;
    this.aggregationOptions = aggregationOptions;
  }

  @Override
  public CollectionName getCollectionName() {
    return collectionName;
  }

  @Override
  public Query generateQuery(String readSpecificationName) {
    return new Aggregation(readSpecificationName, aggregationOptions);
  }
}
