package de.claasklar.primitives.document;

public sealed interface Value
    permits ArrayValue, BoolValue, ByteValue, Id, NumberValue, ObjectValue, StringValue {
  Object toBasicType();
}
