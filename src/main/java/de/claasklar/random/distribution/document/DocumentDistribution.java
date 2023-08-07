package de.claasklar.random.distribution.document;

import de.claasklar.phase.PhaseTopic;
import de.claasklar.primitives.CollectionName;
import de.claasklar.random.distribution.DistributionProperties;
import de.claasklar.util.Observer;
import io.opentelemetry.api.trace.Span;
import java.util.List;

public interface DocumentDistribution extends Observer<PhaseTopic.BenchmarkPhase> {
  DocumentRunnable next(Span span);

  CollectionName getCollectionName();

  List<DistributionProperties> distributionProperties();
}
