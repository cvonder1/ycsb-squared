package de.claasklar.primitives.query;

import de.claasklar.primitives.document.NestedObjectValue;
import de.claasklar.primitives.document.Value;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

public class AggregationOptions {
  private final String aggregate;
  private final List<? extends Value> pipeline;
  private final Boolean allowDiskUse;
  private final Duration maxTime;
  private final Duration maxAwaitTime;
  private final Boolean bypassDocumentValidation;
  private final Collation collation;
  private final NestedObjectValue hint;
  private final String hintString;
  private final NestedObjectValue variables;

  private AggregationOptions(
      String aggregate,
      List<? extends Value> pipeline,
      Boolean allowDiskUse,
      Duration maxTime,
      Duration maxAwaitTime,
      Boolean bypassDocumentValidation,
      Collation collation,
      NestedObjectValue hint,
      String hintString,
      NestedObjectValue variables) {
    this.aggregate = aggregate;
    this.pipeline = pipeline;
    this.allowDiskUse = allowDiskUse;
    this.maxTime = maxTime;
    this.maxAwaitTime = maxAwaitTime;
    this.bypassDocumentValidation = bypassDocumentValidation;
    this.collation = collation;
    this.hint = hint;
    this.hintString = hintString;
    this.variables = variables;
  }

  public static AggregationOptions aggregate(String aggregate) {
    return new AggregationOptions(
        aggregate, Collections.emptyList(), null, null, null, null, null, null, null, null);
  }

  public AggregationOptions pipeline(List<? extends Value> pipeline) {
    return new AggregationOptions(
        aggregate,
        pipeline,
        allowDiskUse,
        maxTime,
        maxAwaitTime,
        bypassDocumentValidation,
        collation,
        hint,
        hintString,
        variables);
  }

  public AggregationOptions allowDiskUse(boolean allowDiskUse) {
    return new AggregationOptions(
        aggregate,
        pipeline,
        allowDiskUse,
        maxTime,
        maxAwaitTime,
        bypassDocumentValidation,
        collation,
        hint,
        hintString,
        variables);
  }

  public AggregationOptions maxTime(Duration maxTime) {
    return new AggregationOptions(
        aggregate,
        pipeline,
        allowDiskUse,
        maxTime,
        maxAwaitTime,
        bypassDocumentValidation,
        collation,
        hint,
        hintString,
        variables);
  }

  public AggregationOptions maxAwaitTime(Duration maxAwaitTime) {
    return new AggregationOptions(
        aggregate,
        pipeline,
        allowDiskUse,
        maxTime,
        maxAwaitTime,
        bypassDocumentValidation,
        collation,
        hint,
        hintString,
        variables);
  }

  public AggregationOptions bypassDocumentValidation(Boolean bypassDocumentValidation) {
    return new AggregationOptions(
        aggregate,
        pipeline,
        allowDiskUse,
        maxTime,
        maxAwaitTime,
        bypassDocumentValidation,
        collation,
        hint,
        hintString,
        variables);
  }

  public AggregationOptions collation(Collation collation) {
    return new AggregationOptions(
        aggregate,
        pipeline,
        allowDiskUse,
        maxTime,
        maxAwaitTime,
        bypassDocumentValidation,
        collation,
        hint,
        hintString,
        variables);
  }

  public AggregationOptions hint(NestedObjectValue hint) {
    return new AggregationOptions(
        aggregate,
        pipeline,
        allowDiskUse,
        maxTime,
        maxAwaitTime,
        bypassDocumentValidation,
        collation,
        hint,
        hintString,
        variables);
  }

  public AggregationOptions hintString(String hintString) {
    return new AggregationOptions(
        aggregate,
        pipeline,
        allowDiskUse,
        maxTime,
        maxAwaitTime,
        bypassDocumentValidation,
        collation,
        hint,
        hintString,
        variables);
  }

  public AggregationOptions variables(NestedObjectValue variables) {
    return new AggregationOptions(
        aggregate,
        pipeline,
        allowDiskUse,
        maxTime,
        maxAwaitTime,
        bypassDocumentValidation,
        collation,
        hint,
        hintString,
        variables);
  }

  public String getAggregate() {
    return aggregate;
  }

  public List<? extends Value> getPipeline() {
    return pipeline;
  }

  public Boolean getAllowDiskUse() {
    return allowDiskUse;
  }

  public Duration getMaxTime() {
    return maxTime;
  }

  public Duration getMaxAwaitTime() {
    return maxAwaitTime;
  }

  public Boolean getBypassDocumentValidation() {
    return bypassDocumentValidation;
  }

  public Collation getCollation() {
    return collation;
  }

  public NestedObjectValue getHint() {
    return hint;
  }

  public String getHintString() {
    return hintString;
  }

  public NestedObjectValue getVariables() {
    return variables;
  }
}
