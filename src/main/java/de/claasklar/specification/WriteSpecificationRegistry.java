package de.claasklar.specification;

import de.claasklar.primitives.CollectionName;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class WriteSpecificationRegistry {
  private final Map<CollectionName, WriteSpecification> writeSpecificationMap;

  public WriteSpecificationRegistry() {
    this.writeSpecificationMap = new HashMap<>(5);
  }

  public Optional<WriteSpecification> get(CollectionName name) {
    var specificaiton = writeSpecificationMap.get(name);
    if (specificaiton == null) {
      return Optional.empty();
    } else {
      return Optional.of(specificaiton);
    }
  }

  public void register(WriteSpecification specification) {
    writeSpecificationMap.put(specification.getCollectionName(), specification);
  }
}
