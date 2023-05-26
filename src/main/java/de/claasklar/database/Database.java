package de.claasklar.database;

import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.Id;
import de.claasklar.primitives.document.OurDocument;
import io.opentelemetry.api.trace.Span;
import java.util.Optional;

public interface Database {

  OurDocument write(CollectionName collectionName, OurDocument document, Span span);

  Optional<OurDocument> read(CollectionName collectionName, Id id, Span span);
}
