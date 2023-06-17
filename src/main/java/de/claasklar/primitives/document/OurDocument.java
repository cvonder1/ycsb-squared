package de.claasklar.primitives.document;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

public final class OurDocument implements ObjectValue {

  private static final String ID_FIELD_NAME = "_id";
  private final Id id;
  private final Map<String, Value> values;

  public OurDocument(Id id, Map<String, Value> values) {
    this.id = id;
    this.values = values;
  }

  @Override
  public void put(String key, Value value) {
    if (key.equals(ID_FIELD_NAME)) {
      throw new IllegalArgumentException("cannot reassign id");
    }
    values.put(key, value);
  }

  public Id getId() {
    return this.id;
  }

  @Override
  public Value get(String key) {
    if (key.equals(ID_FIELD_NAME)) {
      return this.id;
    } else {
      return values.get(key);
    }
  }

  public Map<String, Value> getValues() {
    return Collections.unmodifiableMap(values);
  }

  @Override
  public Set<Entry<String, Value>> entrySet() {
    var entrySet = new HashSet<>(values.entrySet());
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
    OurDocument document = (OurDocument) o;
    return id.equals(document.id) && values.equals(document.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, values);
  }

  @Override
  public String toString() {
    return "OurDocument{" + "id=" + id + ", values=" + values + '}';
  }
}
