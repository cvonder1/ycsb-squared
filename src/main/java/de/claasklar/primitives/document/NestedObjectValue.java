package de.claasklar.primitives.document;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

public final class NestedObjectValue implements ObjectValue {
  private final Map<String, Value> values;

  public static NestedObjectValue object(String key, Value value) {
    return new NestedObjectValue(key, value);
  }

  public static NestedObjectValue object(String key1, Value value1, String key2, Value value2) {
    var object = new NestedObjectValue();
    object.put(key1, value1);
    object.put(key2, value2);
    return object;
  }

  public static NestedObjectValue object(Map<String, Value> values) {
    return new NestedObjectValue(values);
  }

  public NestedObjectValue(String key, Value value) {
    this();
    this.put(key, value);
  }

  public NestedObjectValue() {
    this(new HashMap<>());
  }

  public NestedObjectValue(Map<String, Value> values) {
    this.values = values;
  }

  @Override
  public Value get(String key) {
    return values.get(key);
  }

  @Override
  public void put(String key, Value value) {
    values.put(key, value);
  }

  @Override
  public Set<Entry<String, Value>> entrySet() {
    return values.entrySet();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NestedObjectValue that = (NestedObjectValue) o;
    return Objects.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(values);
  }

  @Override
  public String toString() {
    return "NestedObjectValue{" + "values=" + values + '}';
  }
}
