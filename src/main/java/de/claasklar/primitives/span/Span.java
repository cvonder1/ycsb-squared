package de.claasklar.primitives.span;

import java.time.Clock;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class Span implements AutoCloseable {

  /** human-readable name */
  private final String name;

  private final UUID id;
  private final List<Span> children;
  private final Clock clock;

  private long startTime = -1;
  private long stopTime = -1;

  private Span(String name) {
    this(name, Clock.systemUTC());
  }

  public Span(Class<?> c, String detail) {
    this(c.getName() + " - " + detail);
  }

  private Span(String name, Clock clock) {
    this.name = name;
    this.id = UUID.randomUUID();
    this.children = new LinkedList<>();
    this.clock = clock;
  }

  /**
   * Registers child as child of this Span
   *
   * @param child
   * @return Child
   */
  public Span register(Span child) {
    this.children.add(child);
    return child;
  }

  public Span newChild(Class<?> c, String detail) {
    return this.register(new Span(c, detail));
  }

  /**
   * Starts execution timer for this span
   *
   * @return this
   */
  public Span enter() {
    if (this.startTime != -1) {
      throw new IllegalStateException("cannot start span twice");
    }
    this.startTime = this.clock.millis();
    return this;
  }

  public void exit() {
    if (this.startTime == -1) {
      throw new IllegalStateException("cannot exit span that was not started");
    }
    this.stopTime = this.clock.millis();
  }

  @Override
  public void close() {
    this.exit();
  }
}
