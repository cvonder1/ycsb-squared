package de.claasklar.idStore;

import com.zaxxer.sparsebits.SparseBitSet;
import de.claasklar.primitives.CollectionName;
import de.claasklar.util.Pair;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SparseInMemoryIdStore implements IdStore {

  private final Map<CollectionName, Pair<ReadWriteLock, SparseBitSet>> collections;

  public SparseInMemoryIdStore() {
    this.collections = new HashMap<>();
  }

  @Override
  public void store(CollectionName collectionName, long id) {
    var lockAndList = this.collections.get(collectionName);
    if (lockAndList == null) {
      lockAndList = registerNewCollection(collectionName);
    }
    lockAndList.first().writeLock().lock();
    try {
      lockAndList.second().set((int) id);
    } finally {
      lockAndList.first().writeLock().unlock();
    }
  }

  @Override
  public boolean exists(CollectionName collectionName, long id) {
    var lockAndList = collections.get(collectionName);
    if (lockAndList == null) {
      return false;
    }
    lockAndList.first().readLock().lock();
    try {
      return lockAndList.second().get((int) id);
    } finally {
      lockAndList.first().readLock().unlock();
    }
  }

  private synchronized Pair<ReadWriteLock, SparseBitSet> registerNewCollection(
      CollectionName collectionName) {
    var lockAndList = collections.get(collectionName);
    if (lockAndList == null) {
      lockAndList = new Pair<>(new ReentrantReadWriteLock(), new SparseBitSet());
      collections.put(collectionName, lockAndList);
    }
    return lockAndList;
  }
}
