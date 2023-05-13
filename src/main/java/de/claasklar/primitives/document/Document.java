package de.claasklar.primitives.document;

import java.util.Map;

public record Document(Id id, Map<String, Value> values) {}
