package de.claasklar.random.distribution.reference;

import de.claasklar.primitives.document.Document;
import de.claasklar.random.distribution.document.DocumentRunnable;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class ReferencesRunnable {

  private final DocumentRunnable[] documentRunnables;
  private boolean wasRun = false;

  public ReferencesRunnable(DocumentRunnable[] documentRunnables) {
    this.documentRunnables = documentRunnables;
  }

  public Document[] execute(Executor executor) {
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
            .toArray(Document[]::new);
    this.wasRun = true;
    return documents;
  }
}
