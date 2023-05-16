package de.claasklar.generation;

import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.Document;
import de.claasklar.primitives.document.Id;
import java.util.Map;

public interface DocumentGenerator {

  Document generateDocument(Id id);

  Document generateDocument(Id id, Map<CollectionName, Document[]> references);
}
