package de.claasklar.primitives.index;

import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.NestedObjectValue;
import de.claasklar.primitives.query.Collation;
import java.time.Duration;

public class IndexConfiguration {
  private final CollectionName collectionName;
  private final NestedObjectValue keys;
  private final Boolean background;
  private final Boolean unique;
  private final String name;
  private final Boolean sparse;
  private final Duration expireAfterDuration;
  private final Integer version;
  private final NestedObjectValue weights;
  private final String defaultLanguage;
  private final String languageOverride;
  private final Integer textVersion;
  private final Integer sphereVersion;
  private final Integer bits;
  private final Double min;
  private final Double max;
  private final NestedObjectValue storageEngine;
  private final NestedObjectValue partialFilterExpression;
  private final Collation collation;
  private final NestedObjectValue wildcardProjection;
  private final Boolean hidden;

  IndexConfiguration(
      CollectionName collectionName,
      NestedObjectValue keys,
      Boolean background,
      Boolean unique,
      String name,
      Boolean sparse,
      Duration expireAfterDuration,
      Integer version,
      NestedObjectValue weights,
      String defaultLanguage,
      String languageOverride,
      Integer textVersion,
      Integer sphereVersion,
      Integer bits,
      Double min,
      Double max,
      NestedObjectValue storageEngine,
      NestedObjectValue partialFilterExpression,
      Collation collation,
      NestedObjectValue wildcardProjection,
      Boolean hidden) {
    this.collectionName = collectionName;
    this.keys = keys;
    this.background = background;
    this.unique = unique;
    this.name = name;
    this.sparse = sparse;
    this.expireAfterDuration = expireAfterDuration;
    this.version = version;
    this.weights = weights;
    this.defaultLanguage = defaultLanguage;
    this.languageOverride = languageOverride;
    this.textVersion = textVersion;
    this.sphereVersion = sphereVersion;
    this.bits = bits;
    this.min = min;
    this.max = max;
    this.storageEngine = storageEngine;
    this.partialFilterExpression = partialFilterExpression;
    this.collation = collation;
    this.wildcardProjection = wildcardProjection;
    this.hidden = hidden;
  }

  public CollectionName getCollectionName() {
    return collectionName;
  }

  public NestedObjectValue getKeys() {
    return keys;
  }

  public Boolean getBackground() {
    return background;
  }

  public Boolean getUnique() {
    return unique;
  }

  public String getName() {
    return name;
  }

  public Boolean getSparse() {
    return sparse;
  }

  public Duration getExpireAfterDuration() {
    return expireAfterDuration;
  }

  public Integer getVersion() {
    return version;
  }

  public NestedObjectValue getWeights() {
    return weights;
  }

  public String getDefaultLanguage() {
    return defaultLanguage;
  }

  public String getLanguageOverride() {
    return languageOverride;
  }

  public Integer getTextVersion() {
    return textVersion;
  }

  public Integer getSphereVersion() {
    return sphereVersion;
  }

  public Integer getBits() {
    return bits;
  }

  public Double getMin() {
    return min;
  }

  public Double getMax() {
    return max;
  }

  public NestedObjectValue getStorageEngine() {
    return storageEngine;
  }

  public NestedObjectValue getPartialFilterExpression() {
    return partialFilterExpression;
  }

  public Collation getCollation() {
    return collation;
  }

  public NestedObjectValue getWildcardProjection() {
    return wildcardProjection;
  }

  public Boolean getHidden() {
    return hidden;
  }
}
