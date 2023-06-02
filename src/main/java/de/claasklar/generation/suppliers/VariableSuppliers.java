package de.claasklar.generation.suppliers;

import de.claasklar.idStore.IdStore;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.IdLong;
import de.claasklar.primitives.document.NestedObjectValue;
import de.claasklar.random.distribution.id.IdDistribution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class VariableSuppliers {
  private static final Logger logger = LoggerFactory.getLogger(VariableSuppliers.class);
  private final IdStore idStore;

  public VariableSuppliers(IdStore idStore) {
    this.idStore = idStore;
  }

  public VariableSupplier existingId(
      String variableName, CollectionName collectionName, IdDistribution idDistribution) {
    return () -> {
      IdLong id;
      do {
        id = idDistribution.next();
        IdLong finalId = id;
        logger.atTrace().log(() -> "requested next id for existing id supplier " + finalId.toString());
      } while (!idStore.exists(collectionName, id.id()));
      IdLong finalId1 = id;
      logger.atTrace().log(() -> "found id for existing id supplier: " + finalId1);
      return new NestedObjectValue(Map.of(variableName, id.toId()));
    };
  }
}
