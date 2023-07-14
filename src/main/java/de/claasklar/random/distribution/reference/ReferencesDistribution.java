package de.claasklar.random.distribution.reference;

import de.claasklar.primitives.CollectionName;
import de.claasklar.random.distribution.DistributionProperties;
import io.opentelemetry.api.trace.Span;
import java.util.List;

public interface ReferencesDistribution {
  ReferencesRunnable next(Span span);

  CollectionName getCollectionName();

  List<DistributionProperties> distributionProperties();
}
