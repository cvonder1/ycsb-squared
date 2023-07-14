package de.claasklar.random.distribution;

import java.util.Collections;
import java.util.List;

public enum DistributionProperties {
  // returns the same outcome if called with the same seed
  REPEATABLE;

  public static List<DistributionProperties> and(
      List<DistributionProperties> first, List<DistributionProperties> second) {
    if (first.contains(DistributionProperties.REPEATABLE)
        && second.contains(DistributionProperties.REPEATABLE)) {
      return List.of(DistributionProperties.REPEATABLE);
    } else {
      return Collections.emptyList();
    }
  }
}
