package de.claasklar.generation;

import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.query.Query;

public interface QueryGenerator {
  CollectionName getCollectionName();

  Query generateQuery(String readSpecificationName);
}
