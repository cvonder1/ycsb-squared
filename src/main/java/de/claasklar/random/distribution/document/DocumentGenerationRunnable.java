package de.claasklar.random.distribution.document;

import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.IdLong;
import de.claasklar.primitives.document.OurDocument;
import de.claasklar.specification.DocumentGenerationSpecificationRegistry;
import io.opentelemetry.api.trace.Span;

public final class DocumentGenerationRunnable implements DocumentRunnable {

  private final CollectionName collectionName;
  private final IdLong id;
  private final Span parentSpan;
  private final DocumentGenerationSpecificationRegistry registry;

  private OurDocument document;
  private boolean wasRun = false;

  public DocumentGenerationRunnable(
      CollectionName collectionName,
      IdLong id,
      Span parentSpan,
      DocumentGenerationSpecificationRegistry registry) {
    this.collectionName = collectionName;
    this.id = id;
    this.parentSpan = parentSpan;
    this.registry = registry;
  }

  @Override
  public void run() {
    var writeSpec =
        registry
            .get(collectionName)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Could not find WriteSpecificaiton for collection " + collectionName));
    var runnable = writeSpec.runnable(this.id, parentSpan);
    runnable.run();
    this.document = runnable.getDocument();
    this.wasRun = true;
  }

  @Override
  public OurDocument getDocument() {
    if (!wasRun) {
      throw new IllegalStateException("Cannot get OurDocument before Runnable was run");
    }
    return this.document;
  }

  @Override
  public boolean wasRun() {
    return this.wasRun;
  }
}
