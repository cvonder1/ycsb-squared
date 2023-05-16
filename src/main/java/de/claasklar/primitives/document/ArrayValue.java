package de.claasklar.primitives.document;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class ArrayValue implements Value, Iterable<Value> {
  private final List<Value> values;

  public ArrayValue(List<Value> values) {
    this.values = new LinkedList<>(values);
  }

  public ArrayValue() {
    this.values = new LinkedList<>();
  }

  public void add(Value value) {
    this.values.add(value);
  }

  @Override
  public Iterator<Value> iterator() {
    return values.iterator();
  }

  @Override
  public Object toBasicType() {
    return values.stream().map(Value::toBasicType).collect(Collectors.toList());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ArrayValue values1 = (ArrayValue) o;
    return values.equals(values1.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(values);
  }
}
