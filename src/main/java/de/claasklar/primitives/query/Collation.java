package de.claasklar.primitives.query;

public class Collation {
  private final String locale;
  private final Boolean caseLevel;
  private final CollationCaseFirst caseFirst;
  private final CollationStrength strength;
  private final Boolean numericOrdering;
  private final CollationAlternate alternate;
  private final CollationMaxVariable maxVariable;
  private final Boolean normalization;
  private final Boolean backwards;

  private Collation(
      String locale,
      Boolean caseLevel,
      CollationCaseFirst caseFirst,
      CollationStrength strength,
      Boolean numericOrdering,
      CollationAlternate alternate,
      CollationMaxVariable maxVariable,
      Boolean normalization,
      Boolean backwards) {
    this.locale = locale;
    this.caseLevel = caseLevel;
    this.caseFirst = caseFirst;
    this.strength = strength;
    this.numericOrdering = numericOrdering;
    this.alternate = alternate;
    this.maxVariable = maxVariable;
    this.normalization = normalization;
    this.backwards = backwards;
  }

  public static Collation collation() {
    return new Collation(null, null, null, null, null, null, null, null, null);
  }

  public Collation locale(String locale) {
    return new Collation(
        locale,
        caseLevel,
        caseFirst,
        strength,
        numericOrdering,
        alternate,
        maxVariable,
        normalization,
        backwards);
  }

  public Collation caseLevel(boolean caseLevel) {
    return new Collation(
        locale,
        caseLevel,
        caseFirst,
        strength,
        numericOrdering,
        alternate,
        maxVariable,
        normalization,
        backwards);
  }

  public Collation caseFirst(CollationCaseFirst caseFirst) {
    return new Collation(
        locale,
        caseLevel,
        caseFirst,
        strength,
        numericOrdering,
        alternate,
        maxVariable,
        normalization,
        backwards);
  }

  public Collation strength(CollationStrength strength) {
    return new Collation(
        locale,
        caseLevel,
        caseFirst,
        strength,
        numericOrdering,
        alternate,
        maxVariable,
        normalization,
        backwards);
  }

  public Collation numericOrdering(Boolean numericOrdering) {
    return new Collation(
        locale,
        caseLevel,
        caseFirst,
        strength,
        numericOrdering,
        alternate,
        maxVariable,
        normalization,
        backwards);
  }

  public Collation alternate(CollationAlternate alternate) {
    return new Collation(
        locale,
        caseLevel,
        caseFirst,
        strength,
        numericOrdering,
        alternate,
        maxVariable,
        normalization,
        backwards);
  }

  public Collation maxVariable(CollationMaxVariable maxVariable) {
    return new Collation(
        locale,
        caseLevel,
        caseFirst,
        strength,
        numericOrdering,
        alternate,
        maxVariable,
        normalization,
        backwards);
  }

  public Collation normalization(boolean normalization) {
    return new Collation(
        locale,
        caseLevel,
        caseFirst,
        strength,
        numericOrdering,
        alternate,
        maxVariable,
        normalization,
        backwards);
  }

  public Collation backwards(boolean backwards) {
    return new Collation(
        locale,
        caseLevel,
        caseFirst,
        strength,
        numericOrdering,
        alternate,
        maxVariable,
        normalization,
        backwards);
  }

  public String getLocale() {
    return locale;
  }

  public Boolean getCaseLevel() {
    return caseLevel;
  }

  public CollationCaseFirst getCaseFirst() {
    return caseFirst;
  }

  public CollationStrength getStrength() {
    return strength;
  }

  public Boolean getNumericOrdering() {
    return numericOrdering;
  }

  public CollationAlternate getAlternate() {
    return alternate;
  }

  public CollationMaxVariable getMaxVariable() {
    return maxVariable;
  }

  public Boolean getNormalization() {
    return normalization;
  }

  public Boolean getBackwards() {
    return backwards;
  }
}
