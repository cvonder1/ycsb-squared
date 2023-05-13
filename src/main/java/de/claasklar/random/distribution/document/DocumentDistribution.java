package de.claasklar.random.distribution.document;

import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.span.Span;

public interface DocumentDistribution {
  DocumentRunnable next(Span span);

  CollectionName getCollectionName();
}
