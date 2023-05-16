package de.claasklar.primitives.document;

public record NumberValue(float value) implements Value {

  @Override
  public Object toBasicType() {
    return value;
  }
}
