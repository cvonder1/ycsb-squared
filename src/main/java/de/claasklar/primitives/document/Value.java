package de.claasklar.primitives.document;

public sealed interface Value
    permits ArrayValue,
        BoolValue,
        ByteValue,
        Id,
        DoubleValue,
        ObjectValue,
        StringValue,
        NullValue,
        IntValue {
  Object toBasicType();
}
