package de.claasklar.primitives.document;

public final record NullValue() implements Value {

  public static final NullValue VALUE = new NullValue();

  public static NullValue nill() {
    return NullValue.VALUE;
  }

  @Override
  public Object toBasicType() {
    return null;
  }
}
