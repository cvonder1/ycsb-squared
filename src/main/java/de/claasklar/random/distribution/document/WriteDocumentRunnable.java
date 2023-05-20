package de.claasklar.random.distribution.document;

import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.Document;
import de.claasklar.primitives.document.IdLong;
import de.claasklar.specification.WriteSpecificationRegistry;
import io.opentelemetry.api.trace.Span;

/** For new documents */
public final class WriteDocumentRunnable implements DocumentRunnable {

  private final CollectionName collectionName;
  private final IdLong id;
  private final Span parentSpan;
  private final WriteSpecificationRegistry registry;

  private Document document;
  private boolean wasRun = false;

  public WriteDocumentRunnable(
      CollectionName collectionName,
      IdLong id,
      Span parentSpan,
      WriteSpecificationRegistry registry) {
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
  public Document getDocument() {
    if (!wasRun) {
      throw new IllegalStateException("Cannot get Document before Runnable was run");
    }
    return this.document;
  }

  @Override
  public boolean wasRun() {
    return this.wasRun;
  }

  public CollectionName getCollectionName() {
    if (!wasRun) {
      throw new IllegalStateException("Cannot get collection name before Runnable was run");
    }
    return collectionName;
  }

  public IdLong getId() {
    if (!wasRun) {
      throw new IllegalStateException("Cannot get id before Runnable was run");
    }
    return id;
  }
}
