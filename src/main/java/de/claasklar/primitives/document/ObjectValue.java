package de.claasklar.primitives.document;

import java.util.Map;

public record ObjectValue(Map<String, Value> value) implements Value {}
