package de.claasklar.generation;

import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.NestedObjectValue;
import de.claasklar.primitives.query.Aggregation;
import de.claasklar.primitives.query.AggregationOptions;
import de.claasklar.primitives.query.Query;
import java.util.function.Supplier;

/**
 * A {@link QueryGenerator} implementation that returns an aggregation query with changeable variables.
 */
public class VariableAggregationGenerator implements QueryGenerator {

  private final CollectionName collectionName;
  private final AggregationOptions aggregationOptions;
  private final Supplier<NestedObjectValue> variableSupplier;

  public VariableAggregationGenerator(
      CollectionName collectionName,
      AggregationOptions aggregationOptions,
      Supplier<NestedObjectValue> variableSupplier) {
    this.collectionName = collectionName;
    this.aggregationOptions = aggregationOptions;
    this.variableSupplier = variableSupplier;
  }

  @Override
  public CollectionName getCollectionName() {
    return collectionName;
  }

  @Override
  public Query generateQuery(String readSpecificationName) {
    return new Aggregation(
        readSpecificationName, aggregationOptions.variables(variableSupplier.get()));
  }
}
