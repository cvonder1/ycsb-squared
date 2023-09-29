package de.claasklar.generation;

import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.NestedObjectValue;
import de.claasklar.primitives.query.Find;
import de.claasklar.primitives.query.FindOptions;
import de.claasklar.primitives.query.Query;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link QueryGenerator} implementation that returns a find query with changeable variables.
 */
public class VariableFindGenerator implements QueryGenerator {

  private static final Logger logger = LoggerFactory.getLogger(VariableFindGenerator.class);

  private final CollectionName collectionName;
  private final FindOptions findOptions;
  private final Supplier<NestedObjectValue> variableSupplier;

  public VariableFindGenerator(
      CollectionName collectionName,
      FindOptions findOptions,
      Supplier<NestedObjectValue> variableSupplier) {
    this.collectionName = collectionName;
    this.findOptions = findOptions;
    this.variableSupplier = variableSupplier;
  }

  @Override
  public Query generateQuery(String readSpecificationName) {
    logger.atDebug().log(() -> "generating query for " + readSpecificationName);
    return new Find(
        collectionName, readSpecificationName, findOptions.variables(variableSupplier.get()));
  }

  @Override
  public CollectionName getCollectionName() {
    return collectionName;
  }
}
