package de.claasklar.idStore;

import de.claasklar.primitives.CollectionName;
import de.claasklar.util.Pair;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryIdStore implements IdStore {

  private final Map<CollectionName, Pair<ReadWriteLock, ArrayWrapper>> collections;
  private final short[] filters =
      new short[] {
        0b0000_0001,
        0b0000_0010,
        0b0000_0100,
        0b0000_1000,
        0b0001_0000,
        0b0010_0000,
        0b0100_0000,
        0b1000_0000
      };

  public InMemoryIdStore() {
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
      var list = lockAndList.second().array;
      long pos = id / 8;
      if (pos > Integer.MAX_VALUE) {
        throw new UnsupportedOperationException(
            "ids are only supported up to " + Integer.MAX_VALUE);
      }
      if (pos >= list.length) {
        list = Arrays.copyOf(list, (int) pos + 1);
        lockAndList.second().array = list;
      }
      short filter = filters[(int) (id % 8)];
      list[(int) pos] |= filter;
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
      var list = lockAndList.second().array;
      if ((id / 8) > Integer.MAX_VALUE) {
        throw new UnsupportedOperationException(
            "ids are only supported up to " + Integer.MAX_VALUE);
      }
      byte byteContainingId = list[(int) (id / 8)];
      var filter = filters[(int) (id % 8)];
      short unsignedRes = (short) (byteContainingId & filter);
      return unsignedRes > 0;
    } catch (ArrayIndexOutOfBoundsException e) {
      return false;
    } finally {
      lockAndList.first().readLock().unlock();
    }
  }

  private synchronized Pair<ReadWriteLock, ArrayWrapper> registerNewCollection(
      CollectionName collectionName) {
    var lockAndList = collections.get(collectionName);
    if (lockAndList == null) {
      lockAndList = new Pair<>(new ReentrantReadWriteLock(), new ArrayWrapper(new byte[0]));
      collections.put(collectionName, lockAndList);
    }
    return lockAndList;
  }

  private static class ArrayWrapper {
    byte[] array;

    public ArrayWrapper(byte[] array) {
      this.array = array;
    }
  }
}
