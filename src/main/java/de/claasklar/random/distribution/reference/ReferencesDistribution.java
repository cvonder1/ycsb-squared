package de.claasklar.random.distribution.reference;

import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.span.Span;

public interface ReferencesDistribution {
  ReferencesRunnable next(Span span);

  CollectionName getCollectionName();
}
