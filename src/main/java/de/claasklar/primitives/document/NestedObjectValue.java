package de.claasklar.primitives.document;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

public final class NestedObjectValue implements ObjectValue {
  private final Map<String, Value> values;

  public NestedObjectValue() {
    this(new HashMap<>());
  }

  public NestedObjectValue(Map<String, Value> values) {
    this.values = values;
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
