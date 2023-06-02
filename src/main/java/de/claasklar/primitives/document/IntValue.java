package de.claasklar.primitives.document;

public final record IntValue(int value) implements Value {
  @Override
  public Object toBasicType() {
    return value;
  }
}
