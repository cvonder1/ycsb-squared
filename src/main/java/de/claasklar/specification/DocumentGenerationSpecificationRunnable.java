package de.claasklar.specification;

import de.claasklar.primitives.document.OurDocument;

public interface DocumentGenerationSpecificationRunnable extends Runnable {
  OurDocument getDocument();
}
