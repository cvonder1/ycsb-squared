package de.claasklar.primitives.document;

import java.util.Arrays;

public record Id(byte[] id) implements Value {
  public Id {
    if (id.length != 12) {
      throw new IllegalArgumentException("id must have 12 bytes, it has " + id.length + " bytes");
    }
  }

  @Override
  public Object toBasicType() {
    return id;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Id o = (Id) obj;
    return Arrays.equals(this.id, o.id);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.id);
  }
}
