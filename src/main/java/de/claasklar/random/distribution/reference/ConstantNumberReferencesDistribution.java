package de.claasklar.random.distribution.reference;

import de.claasklar.primitives.CollectionName;
import de.claasklar.random.distribution.document.DocumentDistribution;
import de.claasklar.random.distribution.document.DocumentRunnable;
import io.opentelemetry.api.trace.Span;
import java.util.concurrent.ExecutorService;

public class ConstantNumberReferencesDistribution implements ReferencesDistribution {

  private final int constant;
  private final DocumentDistribution documentDistribution;
  private final ExecutorService executorService;

  public ConstantNumberReferencesDistribution(
      int constant, DocumentDistribution documentDistribution, ExecutorService executorService) {
    this.constant = constant;
    this.documentDistribution = documentDistribution;
    this.executorService = executorService;
  }

  @Override
  public ReferencesRunnable next(Span span) {
    var documentRunnables = new DocumentRunnable[constant];
    for (int i = 0; i < constant; i++) {
      documentRunnables[i] = documentDistribution.next(span);
    }
    return new ReferencesRunnable(documentRunnables, executorService);
  }

  @Override
  public CollectionName getCollectionName() {
    return this.documentDistribution.getCollectionName();
  }
}
