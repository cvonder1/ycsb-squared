package de.claasklar.primitives.index;

import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.NestedObjectValue;
import de.claasklar.primitives.query.Collation;
import java.time.Duration;
import java.util.Objects;

public class IndexConfigurationBuilder {
  private CollectionName collectionName;
  private NestedObjectValue keys;
  private Boolean background;
  private Boolean unique;
  private String name;
  private Boolean sparse;
  private Duration expireAfterDuration;
  private Integer version;
  private NestedObjectValue weights;
  private String defaultLanguage;
  private String languageOverride;
  private Integer textVersion;
  private Integer sphereVersion;
  private Integer bits;
  private Double min;
  private Double max;
  private NestedObjectValue storageEngine;
  private NestedObjectValue partialFilterExpression;
  private Collation collation;
  private NestedObjectValue wildcardProjection;
  private Boolean hidden;

  private IndexConfigurationBuilder() {}

  public static IndexConfigurationBuilder builder() {
    return new IndexConfigurationBuilder();
  }

  public IndexConfigurationBuilder collectionName(CollectionName collectionName) {
    this.collectionName = collectionName;
    return this;
  }

  public IndexConfigurationBuilder keys(NestedObjectValue keys) {
    this.keys = keys;
    return this;
  }

  public IndexConfigurationBuilder background(boolean background) {
    this.background = background;
    return this;
  }

  public IndexConfigurationBuilder unique(boolean unique) {
    this.unique = unique;
    return this;
  }

  public IndexConfigurationBuilder name(String name) {
    this.name = name;
    return this;
  }

  public IndexConfigurationBuilder sparse(boolean sparse) {
    this.sparse = sparse;
    return this;
  }

  public IndexConfigurationBuilder expireAfterSeconds(Duration expireAfterDuration) {
    this.expireAfterDuration = expireAfterDuration;
    return this;
  }

  public IndexConfigurationBuilder version(int version) {
    this.version = version;
    return this;
  }

  public IndexConfigurationBuilder weights(NestedObjectValue weights) {
    this.weights = weights;
    return this;
  }

  public IndexConfigurationBuilder defaultLanguage(String defaultLanguage) {
    this.defaultLanguage = defaultLanguage;
    return this;
  }

  public IndexConfigurationBuilder languageOverride(String languageOverride) {
    this.languageOverride = languageOverride;
    return this;
  }

  public IndexConfigurationBuilder textVersion(int textVersion) {
    this.textVersion = textVersion;
    return this;
  }

  public IndexConfigurationBuilder sphereVersion(int sphereVersion) {
    this.sphereVersion = sphereVersion;
    return this;
  }

  public IndexConfigurationBuilder bits(int bits) {
    this.bits = bits;
    return this;
  }

  public IndexConfigurationBuilder min(double min) {
    this.min = min;
    return this;
  }

  public IndexConfigurationBuilder max(double max) {
    this.max = max;
    return this;
  }

  public IndexConfigurationBuilder storageEngine(NestedObjectValue storageEngine) {
    this.storageEngine = storageEngine;
    return this;
  }

  public IndexConfigurationBuilder partialFilterExpression(
      NestedObjectValue partialFilterExpression) {
    this.partialFilterExpression = partialFilterExpression;
    return this;
  }

  public IndexConfigurationBuilder collation(Collation collation) {
    this.collation = collation;
    return this;
  }

  public IndexConfigurationBuilder wildcardProjection(NestedObjectValue wildcardProjection) {
    this.wildcardProjection = wildcardProjection;
    return this;
  }

  public IndexConfigurationBuilder hidden(boolean hidden) {
    this.hidden = hidden;
    return this;
  }

  public IndexConfiguration build() {
    Objects.requireNonNull(keys);
    return new IndexConfiguration(
        collectionName,
        keys,
        background,
        unique,
        name,
        sparse,
        expireAfterDuration,
        version,
        weights,
        defaultLanguage,
        languageOverride,
        textVersion,
        sphereVersion,
        bits,
        min,
        max,
        storageEngine,
        partialFilterExpression,
        collation,
        wildcardProjection,
        hidden);
  }
}
