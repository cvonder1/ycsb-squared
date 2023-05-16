package de.claasklar.primitives.document;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public final class NestedObjectValue implements ObjectValue {
  private final Map<String, Value> values;

  public NestedObjectValue() {
    this.values = new HashMap<>();
  }

  @Override
  public void put(String key, Value value) {
    values.put(key, value);
  }

  @Override
  public Set<Entry<String, Value>> entrySet() {
    return values.entrySet();
  }
}
