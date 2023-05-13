package de.claasklar.random.distribution.document;

import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.Id;

public class NoSuchDocumentException extends RuntimeException {

  public NoSuchDocumentException(CollectionName collectionName, Id id) {
    super("ID " + id + " could not be found in collection " + collectionName);
  }
}
