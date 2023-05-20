package de.claasklar.random.distribution.reference;

import de.claasklar.primitives.CollectionName;
import io.opentelemetry.api.trace.Span;

public interface ReferencesDistribution {
  ReferencesRunnable next(Span span);

  CollectionName getCollectionName();
}
