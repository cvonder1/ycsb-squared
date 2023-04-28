package de.claasklar.idStore;

import de.claasklar.primitives.CollectionName;

public interface IdStore {
  void store(CollectionName collectionName, long id);

  boolean exists(CollectionName collectionName, long id);
}
