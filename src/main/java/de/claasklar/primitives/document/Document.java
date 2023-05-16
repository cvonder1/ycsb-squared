package de.claasklar.primitives.document;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

public record Document(Id id, Map<String, Value> values) implements ObjectValue {

  @Override
  public void put(String key, Value value) {
    values.put(key, value);
  }

  @Override
  public Set<Entry<String, Value>> entrySet() {
    var entrySet = new HashSet(values.entrySet());
    entrySet.add(new SimpleImmutableEntry<>("_id", id));
    return entrySet;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Document document = (Document) o;
    return id.equals(document.id) && values.equals(document.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, values);
  }
}
