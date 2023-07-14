package de.claasklar.random.distribution.id;

import de.claasklar.primitives.document.IdLong;
import de.claasklar.random.distribution.DistributionProperties;
import java.util.List;

public interface IdDistribution {
  default IdLong next() {
    return new IdLong(nextAsLong());
  }

  long nextAsLong();

  List<DistributionProperties> distributionProperties();
}
