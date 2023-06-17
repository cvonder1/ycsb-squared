package de.claasklar.random.distribution.reference;

import de.claasklar.primitives.document.OurDocument;
import de.claasklar.random.distribution.document.DocumentRunnable;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

public class ReferencesRunnable implements Runnable {

  private final DocumentRunnable[] documentRunnables;
  private final ExecutorService executor;
  private boolean wasRun = false;
  private OurDocument[] documents;

  public ReferencesRunnable(DocumentRunnable[] documentRunnables, ExecutorService executor) {
    this.documentRunnables = documentRunnables;
    this.executor = executor;
  }

  public OurDocument[] execute(Executor executor) {
    if (wasRun) {
      throw new IllegalStateException("Cannot execute DocumentListFuture twice");
    }
    var futures =
        Arrays.stream(documentRunnables)
            .map(it -> CompletableFuture.runAsync(it, executor))
            .toArray(CompletableFuture[]::new);
    CompletableFuture.allOf(futures).join();
    var documents =
        Arrays.stream(documentRunnables)
            .map(DocumentRunnable::getDocument)
            .toArray(OurDocument[]::new);
    this.wasRun = true;
    return documents;
  }

  @Override
  public void run() {
    if (wasRun) {
      throw new IllegalStateException("Cannot execute DocumentListFuture twice");
    }
    var futures =
        Arrays.stream(documentRunnables)
            .map(it -> CompletableFuture.runAsync(it, executor))
            .toArray(CompletableFuture[]::new);
    CompletableFuture.allOf(futures).join();
    documents =
        Arrays.stream(documentRunnables)
            .map(DocumentRunnable::getDocument)
            .toArray(OurDocument[]::new);
    this.wasRun = true;
  }

  public OurDocument[] getDocuments() {
    if (!wasRun) {
      throw new IllegalStateException("cannot get documents before it was run");
    }
    return this.documents;
  }
}
