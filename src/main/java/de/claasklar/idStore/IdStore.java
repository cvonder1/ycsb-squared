package de.claasklar.idStore;

import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.IdLong;

public interface IdStore {
  void store(CollectionName collectionName, long id);

  boolean exists(CollectionName collectionName, long id);

  default void store(CollectionName collectionName, IdLong id) {
    store(collectionName, id.id());
  }
}
