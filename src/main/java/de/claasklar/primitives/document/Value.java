package de.claasklar.primitives.document;

public sealed interface Value
    permits ArrayValue,
        BoolValue,
        ByteValue,
        DoubleValue,
        Id,
        IntValue,
        LongValue,
        NullValue,
        ObjectValue,
        StringValue {
  Object toBasicType();
}
