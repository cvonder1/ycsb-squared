package de.claasklar.primitives.document;

public record StringValue(String value) implements Value {

  public static StringValue string(String value) {
    return new StringValue(value);
  }

  @Override
  public Object toBasicType() {
    return value;
  }
}
