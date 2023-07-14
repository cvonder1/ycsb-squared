package de.claasklar.specification;

import de.claasklar.primitives.CollectionName;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class DocumentGenerationSpecificationRegistry {
  private final Map<CollectionName, DocumentGenerationSpecification> specificationMap;

  public DocumentGenerationSpecificationRegistry() {
    this.specificationMap = new HashMap<>();
  }

  /**
   * @param name
   * @return a specification responsible for creating the collection with the given name
   */
  public Optional<DocumentGenerationSpecification> get(CollectionName name) {
    var specification = specificationMap.get(name);
    return Optional.ofNullable(specification);
  }

  public void register(DocumentGenerationSpecification specification) {
    specificationMap.put(specification.getCollectionName(), specification);
  }

  public Set<CollectionName> allWriteSpecificationCollectionNames() {
    return specificationMap.entrySet().stream()
        .filter(it -> it.getValue() instanceof WriteSpecification)
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());
  }
}
