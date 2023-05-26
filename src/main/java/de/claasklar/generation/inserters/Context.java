package de.claasklar.generation.inserters;

import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.OurDocument;
import java.util.Map;

public record Context(Map<CollectionName, OurDocument[]> references) {}
