package de.claasklar.primitives.document;

public final record IntValue(int value) implements Value {

  public static IntValue integer(int value) {
    return new IntValue(value);
  }

  @Override
  public Object toBasicType() {
    return value;
  }
}
