package de.claasklar.primitives.document;

public record BoolValue(boolean value) implements Value {

  @Override
  public Object toBasicType() {
    return value;
  }
}
