package de.claasklar.primitives.document;

public sealed interface Value
    permits BoolValue, ByteValue, Id, NumberValue, ObjectValue, StringValue {}
