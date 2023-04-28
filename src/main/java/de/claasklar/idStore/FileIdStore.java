package de.claasklar.idStore;

import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.Pair;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class FileIdStore implements IdStore {

  private final Map<CollectionName, Pair<Lock, RandomAccessFile>> files;
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

  public FileIdStore() {
    this.files = new HashMap<>(10);
  }

  @Override
  public void store(CollectionName collectionName, long id) {
    var lockAndFile = this.files.get(collectionName);
    if (lockAndFile == null) {
      lockAndFile = this.registerNewCollection(collectionName);
    }
    var lock = lockAndFile.first();
    var file = lockAndFile.second();
    lock.lock();
    try {
      byte[] byteRead;
      try {
        byteRead = this.readByteContainingId(file, id);
      } catch (EOFException ignored) {
        byteRead = new byte[1];
      }
      var pos = (int) id % 8;
      short filter = filters[pos];
      byteRead[0] |= filter;
      file.seek(id / 8);
      file.write(byteRead);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  private synchronized Pair<Lock, RandomAccessFile> registerNewCollection(CollectionName collectionName) {
    var lockAndFile = this.files.get(collectionName);
    if (lockAndFile == null) {
      try {
        lockAndFile =
          new Pair<>(
            new ReentrantLock(),
            new RandomAccessFile(File.createTempFile("ycsb_ids", "raw"), "rw"));
        this.files.put(collectionName, lockAndFile);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return lockAndFile;
  }

  @Override
  public boolean exists(CollectionName collectionName, long id) {
    var lockAndFile = this.files.get(collectionName);
    if (lockAndFile == null) {
      return false;
    }
    var lock = lockAndFile.first();
    var file = lockAndFile.second();
    lock.lock();
    try {
      var byteRead = this.readByteContainingId(file, id);
      var pos = (int) id % 8;
      short filter = this.filters[pos];
      short unsignedRes = (short) (byteRead[0] & filter);
      return unsignedRes > 0;
    } catch (EOFException e) {
      return false;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  private byte[] readByteContainingId(RandomAccessFile file, long id) throws IOException {
    file.seek(id / 8);
    var byteRead = new byte[1];
    file.readFully(byteRead);
    return byteRead;
  }
}
