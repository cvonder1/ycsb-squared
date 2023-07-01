package de.claasklar.primitives.document;

import java.util.Map.Entry;
import java.util.Set;

public final record NullValue() implements Value, ObjectValue {

  public static final NullValue VALUE = new NullValue();

  public static NullValue nill() {
    return NullValue.VALUE;
  }

  @Override
  public Object toBasicType() {
    return null;
  }

  @Override
  public void put(String key, Value value) {
    throw new UnsupportedOperationException("this is null");
  }

  @Override
  public Value get(String key) {
    throw new UnsupportedOperationException("this is null");
  }

  @Override
  public Set<Entry<String, Value>> entrySet() {
    throw new UnsupportedOperationException("this is null");
  }
}
