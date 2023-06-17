package de.claasklar.primitives.document;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

public sealed interface ObjectValue extends Value permits NestedObjectValue, OurDocument {

  void put(String key, Value value);

  Value get(String key);

  Set<Entry<String, Value>> entrySet();

  @Override
  default Object toBasicType() {
    var entrySet = entrySet();
    var map = new HashMap<>(entrySet.size());
    entrySet.forEach(it -> map.put(it.getKey(), it.getValue().toBasicType()));
    return map;
  }
}
