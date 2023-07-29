package de.claasklar.random.distribution;

import java.util.List;

public interface Distribution<T> {
  T sample();

  List<DistributionProperties> distributionProperties();
}
