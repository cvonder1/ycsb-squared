package de.claasklar.random.distribution.document;

import de.claasklar.primitives.CollectionName;
import de.claasklar.random.distribution.DistributionProperties;
import io.opentelemetry.api.trace.Span;
import java.util.List;

public interface DocumentDistribution {
  DocumentRunnable next(Span span);

  CollectionName getCollectionName();

  List<DistributionProperties> distributionProperties();
}
