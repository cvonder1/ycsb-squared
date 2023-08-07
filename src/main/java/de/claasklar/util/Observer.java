package de.claasklar.util;

public interface Observer<T> {
  void update(T update);

  void setSubject(Subject<T> subject);
}
