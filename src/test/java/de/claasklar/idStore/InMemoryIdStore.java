package de.claasklar.idStore;

import de.claasklar.primitives.CollectionName;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class InMemoryIdStore implements IdStore {
  private final Map<CollectionName, Set<Long>> collections = new HashMap<>();

  @Override
  public void store(CollectionName collectionName, long id) {
    var collection = collections.computeIfAbsent(collectionName, (key) -> new HashSet<>());
    collection.add(id);
  }

  @Override
  public boolean exists(CollectionName collectionName, long id) {
    if (collections.containsKey(collectionName)) {
      return collections.get(collectionName).contains(id);
    } else {
      return false;
    }
  }
}
