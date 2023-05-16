package de.claasklar.primitives.document;

public record StringValue(String value) implements Value {

  @Override
  public Object toBasicType() {
    return value;
  }
}
