package de.claasklar.random.distribution.document;

import de.claasklar.primitives.document.Document;

public sealed interface DocumentRunnable extends Runnable
    permits ReadDocumentRunnable, WriteDocumentRunnable {

  /**
   * Returns document, previously obtained by running.
   *
   * @return Doucment, existing or not
   * @throws IllegalStateException if called before run
   */
  Document getDocument();

  /**
   * @return Boolean, indicating if it was run before
   */
  boolean wasRun();
}
