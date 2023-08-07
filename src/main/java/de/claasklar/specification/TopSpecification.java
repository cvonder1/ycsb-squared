package de.claasklar.specification;

import de.claasklar.phase.PhaseTopic;
import de.claasklar.util.Observer;

public interface TopSpecification extends Specification, Observer<PhaseTopic.BenchmarkPhase> {
  Runnable runnable();

  String getName();
}
