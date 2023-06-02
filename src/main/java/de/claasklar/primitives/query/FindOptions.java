package de.claasklar.primitives.query;

import de.claasklar.primitives.document.NestedObjectValue;
import java.time.Duration;

public class FindOptions {

  private final NestedObjectValue filter;
  private final Integer batchSize;
  private final Integer limit;
  private final NestedObjectValue projection;
  private final Duration maxTime;
  private final Duration maxAwaitTime;
  private final Integer skip;
  private final NestedObjectValue sort;
  private final Boolean noCursorTimeout;
  private final Boolean partial;
  private final NestedObjectValue hint;
  private final NestedObjectValue variables;
  private final NestedObjectValue max;
  private final NestedObjectValue min;
  private final Boolean returnKey;
  private final Boolean showRecordId;
  private final Boolean allowDiskUse;

  private FindOptions(
      NestedObjectValue filter,
      Integer batchSize,
      Integer limit,
      NestedObjectValue projection,
      Duration maxTime,
      Duration maxAwaitTime,
      Integer skip,
      NestedObjectValue sort,
      Boolean noCursorTimeout,
      Boolean partial,
      NestedObjectValue hint,
      NestedObjectValue variables,
      NestedObjectValue max,
      NestedObjectValue min,
      Boolean returnKey,
      Boolean showRecordId,
      Boolean allowDiskUse) {
    this.filter = filter;
    this.batchSize = batchSize;
    this.limit = limit;
    this.projection = projection;
    this.maxTime = maxTime;
    this.maxAwaitTime = maxAwaitTime;
    this.skip = skip;
    this.sort = sort;
    this.noCursorTimeout = noCursorTimeout;
    this.partial = partial;
    this.hint = hint;
    this.variables = variables;
    this.max = max;
    this.min = min;
    this.returnKey = returnKey;
    this.showRecordId = showRecordId;
    this.allowDiskUse = allowDiskUse;
  }

  public static FindOptions find() {
    return new FindOptions(
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null);
  }

  public FindOptions filter(NestedObjectValue filter) {
    return new FindOptions(
        filter,
        batchSize,
        limit,
        projection,
        maxTime,
        maxAwaitTime,
        skip,
        sort,
        noCursorTimeout,
        partial,
        hint,
        variables,
        max,
        min,
        returnKey,
        showRecordId,
        allowDiskUse);
  }

  public FindOptions batchSize(int batchSize) {
    return new FindOptions(
        filter,
        batchSize,
        limit,
        projection,
        maxTime,
        maxAwaitTime,
        skip,
        sort,
        noCursorTimeout,
        partial,
        hint,
        variables,
        max,
        min,
        returnKey,
        showRecordId,
        allowDiskUse);
  }

  public FindOptions limit(int limit) {
    return new FindOptions(
        filter,
        batchSize,
        limit,
        projection,
        maxTime,
        maxAwaitTime,
        skip,
        sort,
        noCursorTimeout,
        partial,
        hint,
        variables,
        max,
        min,
        returnKey,
        showRecordId,
        allowDiskUse);
  }

  public FindOptions projection(NestedObjectValue projection) {
    return new FindOptions(
        filter,
        batchSize,
        limit,
        projection,
        maxTime,
        maxAwaitTime,
        skip,
        sort,
        noCursorTimeout,
        partial,
        hint,
        variables,
        max,
        min,
        returnKey,
        showRecordId,
        allowDiskUse);
  }

  public FindOptions maxTimeMS(Duration maxTimeMS) {
    return new FindOptions(
        filter,
        batchSize,
        limit,
        projection,
        maxTimeMS,
        maxAwaitTime,
        skip,
        sort,
        noCursorTimeout,
        partial,
        hint,
        variables,
        max,
        min,
        returnKey,
        showRecordId,
        allowDiskUse);
  }

  public FindOptions maxAwaitTimeMS(Duration maxAwaitTimeMS) {
    return new FindOptions(
        filter,
        batchSize,
        limit,
        projection,
        maxTime,
        maxAwaitTimeMS,
        skip,
        sort,
        noCursorTimeout,
        partial,
        hint,
        variables,
        max,
        min,
        returnKey,
        showRecordId,
        allowDiskUse);
  }

  public FindOptions skip(int skip) {
    return new FindOptions(
        filter,
        batchSize,
        limit,
        projection,
        maxTime,
        maxAwaitTime,
        skip,
        sort,
        noCursorTimeout,
        partial,
        hint,
        variables,
        max,
        min,
        returnKey,
        showRecordId,
        allowDiskUse);
  }

  public FindOptions sort(NestedObjectValue sort) {
    return new FindOptions(
        filter,
        batchSize,
        limit,
        projection,
        maxTime,
        maxAwaitTime,
        skip,
        sort,
        noCursorTimeout,
        partial,
        hint,
        variables,
        max,
        min,
        returnKey,
        showRecordId,
        allowDiskUse);
  }

  public FindOptions noCursorTimeout(boolean noCursorTimeout) {
    return new FindOptions(
        filter,
        batchSize,
        limit,
        projection,
        maxTime,
        maxAwaitTime,
        skip,
        sort,
        noCursorTimeout,
        partial,
        hint,
        variables,
        max,
        min,
        returnKey,
        showRecordId,
        allowDiskUse);
  }

  public FindOptions oplogReplay(boolean oplogReplay) {
    return new FindOptions(
        filter,
        batchSize,
        limit,
        projection,
        maxTime,
        maxAwaitTime,
        skip,
        sort,
        noCursorTimeout,
        partial,
        hint,
        variables,
        max,
        min,
        returnKey,
        showRecordId,
        allowDiskUse);
  }

  public FindOptions partial(boolean partial) {
    return new FindOptions(
        filter,
        batchSize,
        limit,
        projection,
        maxTime,
        maxAwaitTime,
        skip,
        sort,
        noCursorTimeout,
        partial,
        hint,
        variables,
        max,
        min,
        returnKey,
        showRecordId,
        allowDiskUse);
  }

  public FindOptions hint(NestedObjectValue hint) {
    return new FindOptions(
        filter,
        batchSize,
        limit,
        projection,
        maxTime,
        maxAwaitTime,
        skip,
        sort,
        noCursorTimeout,
        partial,
        hint,
        variables,
        max,
        min,
        returnKey,
        showRecordId,
        allowDiskUse);
  }

  public FindOptions variables(NestedObjectValue variables) {
    return new FindOptions(
        filter,
        batchSize,
        limit,
        projection,
        maxTime,
        maxAwaitTime,
        skip,
        sort,
        noCursorTimeout,
        partial,
        hint,
        variables,
        max,
        min,
        returnKey,
        showRecordId,
        allowDiskUse);
  }

  public FindOptions max(NestedObjectValue max) {
    return new FindOptions(
        filter,
        batchSize,
        limit,
        projection,
        maxTime,
        maxAwaitTime,
        skip,
        sort,
        noCursorTimeout,
        partial,
        hint,
        variables,
        max,
        min,
        returnKey,
        showRecordId,
        allowDiskUse);
  }

  public FindOptions min(NestedObjectValue min) {
    return new FindOptions(
        filter,
        batchSize,
        limit,
        projection,
        maxTime,
        maxAwaitTime,
        skip,
        sort,
        noCursorTimeout,
        partial,
        hint,
        variables,
        max,
        min,
        returnKey,
        showRecordId,
        allowDiskUse);
  }

  public FindOptions returnKey(boolean returnKey) {
    return new FindOptions(
        filter,
        batchSize,
        limit,
        projection,
        maxTime,
        maxAwaitTime,
        skip,
        sort,
        noCursorTimeout,
        partial,
        hint,
        variables,
        max,
        min,
        returnKey,
        showRecordId,
        allowDiskUse);
  }

  public FindOptions showRecordId(boolean showRecordId) {
    return new FindOptions(
        filter,
        batchSize,
        limit,
        projection,
        maxTime,
        maxAwaitTime,
        skip,
        sort,
        noCursorTimeout,
        partial,
        hint,
        variables,
        max,
        min,
        returnKey,
        showRecordId,
        allowDiskUse);
  }

  public FindOptions allowDiskUsage(boolean allowDiskUse) {
    return new FindOptions(
        filter,
        batchSize,
        limit,
        projection,
        maxTime,
        maxAwaitTime,
        skip,
        sort,
        noCursorTimeout,
        partial,
        hint,
        variables,
        max,
        min,
        returnKey,
        showRecordId,
        allowDiskUse);
  }

  public NestedObjectValue getFilter() {
    return filter;
  }

  public Integer getBatchSize() {
    return batchSize;
  }

  public Integer getLimit() {
    return limit;
  }

  public NestedObjectValue getProjection() {
    return projection;
  }

  public Duration getMaxTime() {
    return maxTime;
  }

  public Duration getMaxAwaitTime() {
    return maxAwaitTime;
  }

  public Integer getSkip() {
    return skip;
  }

  public NestedObjectValue getSort() {
    return sort;
  }

  public Boolean getNoCursorTimeout() {
    return noCursorTimeout;
  }

  public Boolean getPartial() {
    return partial;
  }

  public NestedObjectValue getHint() {
    return hint;
  }

  public NestedObjectValue getVariables() {
    return variables;
  }

  public NestedObjectValue getMax() {
    return max;
  }

  public NestedObjectValue getMin() {
    return min;
  }

  public Boolean getReturnKey() {
    return returnKey;
  }

  public Boolean getShowRecordId() {
    return showRecordId;
  }

  public Boolean getAllowDiskUse() {
    return allowDiskUse;
  }
}
