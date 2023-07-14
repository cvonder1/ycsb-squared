package de.claasklar.specification;

import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.IdLong;
import io.opentelemetry.api.trace.Span;

public interface DocumentGenerationSpecification extends Specification {
  DocumentGenerationSpecificationRunnable runnable(IdLong id, Span span);

  CollectionName getCollectionName();
}
