package de.claasklar.database;

import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.Document;
import de.claasklar.primitives.document.Id;
import de.claasklar.primitives.span.Span;
import java.util.Optional;

public interface Database {
  Document write(CollectionName collectionName, Document document, Span span);

  Optional<Document> read(CollectionName collectionName, Id id, Span span);
}
