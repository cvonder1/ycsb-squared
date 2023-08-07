package de.claasklar.util;

public interface Subject<T> {
  void register(Observer<T> observer);

  void unregister(Observer<T> observer);

  void notifyObservers(T message);
}
