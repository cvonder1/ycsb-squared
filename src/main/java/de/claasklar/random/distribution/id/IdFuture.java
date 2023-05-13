package de.claasklar.random.distribution.id;

import de.claasklar.primitives.document.Id;

public interface IdFuture {
  Id getId();

  long getIdAsLong();
}
