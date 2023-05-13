package de.claasklar.random.distribution.reference;

import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.span.Span;
import de.claasklar.random.distribution.document.DocumentDistribution;
import de.claasklar.random.distribution.document.DocumentRunnable;

public class ConstantNumberReferencesDistribution implements ReferencesDistribution {

  private final int constant;
  private final DocumentDistribution documentDistribution;

  public ConstantNumberReferencesDistribution(
      int constant, DocumentDistribution documentDistribution) {
    this.constant = constant;
    this.documentDistribution = documentDistribution;
  }

  @Override
  public ReferencesRunnable next(Span span) {
    var documentRunnables = new DocumentRunnable[constant];
    for (int i = 0; i < constant; i++) {
      documentRunnables[i] = documentDistribution.next(span);
    }
    return new ReferencesRunnable(documentRunnables);
  }

  @Override
  public CollectionName getCollectionName() {
    return this.documentDistribution.getCollectionName();
  }
}
