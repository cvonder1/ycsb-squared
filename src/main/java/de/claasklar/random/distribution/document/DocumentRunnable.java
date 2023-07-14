package de.claasklar.random.distribution.document;

import de.claasklar.primitives.document.OurDocument;

public sealed interface DocumentRunnable extends Runnable
    permits ReadDocumentRunnable, WriteDocumentRunnable, DocumentGenerationRunnable {

  /**
   * Returns document, previously obtained by running.
   *
   * @return Doucment, existing or not
   * @throws IllegalStateException if called before run
   */
  OurDocument getDocument();

  /**
   * @return Boolean, indicating if it was run before
   */
  boolean wasRun();
}
