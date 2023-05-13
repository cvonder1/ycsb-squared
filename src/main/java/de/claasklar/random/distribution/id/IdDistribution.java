package de.claasklar.random.distribution.id;

import de.claasklar.primitives.document.IdLong;

public interface IdDistribution {
  default IdLong next() {
    return new IdLong(nextAsLong());
  }

  long nextAsLong();
}
