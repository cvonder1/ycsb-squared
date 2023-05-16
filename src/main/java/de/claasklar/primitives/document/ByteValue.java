package de.claasklar.primitives.document;

import java.util.Arrays;

public record ByteValue(byte[] value) implements Value {

  @Override
  public Object toBasicType() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ByteValue byteValue = (ByteValue) o;
    return Arrays.equals(value, byteValue.value);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(value);
  }
}
