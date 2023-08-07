package de.claasklar.phase;

import de.claasklar.util.Observer;
import de.claasklar.util.Subject;
import java.util.LinkedList;
import java.util.List;

public class PhaseTopic implements Subject<PhaseTopic.BenchmarkPhase> {

  private final List<Observer<BenchmarkPhase>> observers = new LinkedList<>();
  private BenchmarkPhase lastMessage;

  @Override
  public void register(Observer<BenchmarkPhase> observer) {
    if (lastMessage != null) {
      observer.update(lastMessage);
    }
    observers.add(observer);
    observer.setSubject(this);
  }

  @Override
  public void unregister(Observer<BenchmarkPhase> observer) {
    observers.remove(observer);
  }

  @Override
  public void notifyObservers(BenchmarkPhase message) {
    lastMessage = message;
    observers.forEach(it -> it.update(message));
  }

  public enum BenchmarkPhase {
    INDEX,
    LOAD,
    TRANSACTION,
    END
  }
}
