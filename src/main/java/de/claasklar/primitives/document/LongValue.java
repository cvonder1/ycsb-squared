package de.claasklar.primitives.document;

public record LongValue(long value) implements Value {
  @Override
  public Object toBasicType() {
    return value;
  }
}
