package de.claasklar.primitives.document;

public final record NullValue() implements Value {

  public static final NullValue VALUE = new NullValue();

  @Override
  public Object toBasicType() {
    return null;
  }
}
