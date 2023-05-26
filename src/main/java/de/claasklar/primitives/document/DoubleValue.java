package de.claasklar.primitives.document;

public record DoubleValue(double value) implements Value {

  @Override
  public Object toBasicType() {
    return value;
  }
}
