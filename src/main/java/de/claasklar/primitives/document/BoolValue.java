package de.claasklar.primitives.document;

public record BoolValue(boolean value) implements Value {

  public static BoolValue bool(boolean value) {
    return new BoolValue(value);
  }

  @Override
  public Object toBasicType() {
    return value;
  }
}
