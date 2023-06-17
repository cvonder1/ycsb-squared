package de.claasklar.random.distribution;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class SampleNonRepeating<T> {
  private final Distribution<T> sourceDistribution;

  public SampleNonRepeating(Distribution<T> sourceDistribution) {
    this.sourceDistribution = sourceDistribution;
  }

  public List<T> select(int k) {
    var elements = new HashSet<T>(k);
    for (int i = 0; i < k; i++) {
      var next = sourceDistribution.sample();
      if (!elements.add(next)) {
        i--;
      }
    }
    return new ArrayList<>(elements);
  }
}
