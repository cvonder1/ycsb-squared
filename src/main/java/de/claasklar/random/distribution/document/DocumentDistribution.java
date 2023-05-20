package de.claasklar.random.distribution.document;

import de.claasklar.primitives.CollectionName;
import io.opentelemetry.api.trace.Span;

public interface DocumentDistribution {
  DocumentRunnable next(Span span);

  CollectionName getCollectionName();
}
