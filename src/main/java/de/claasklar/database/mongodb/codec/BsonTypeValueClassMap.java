package de.claasklar.database.mongodb.codec;

import de.claasklar.primitives.document.*;
import java.util.Map;
import org.bson.BsonType;

@SuppressWarnings("unchecked")
public class BsonTypeValueClassMap {

  public static final BsonTypeValueClassMap DEFAULT_MAP =
      new BsonTypeValueClassMap(
          Map.of(
              BsonType.STRING,
              StringValue.class,
              BsonType.ARRAY,
              ArrayValue.class,
              BsonType.BOOLEAN,
              BoolValue.class,
              BsonType.BINARY,
              ByteValue.class,
              BsonType.DOUBLE,
              DoubleValue.class,
              BsonType.DOCUMENT,
              NestedObjectValue.class,
              BsonType.NULL,
              NullValue.class,
              BsonType.OBJECT_ID,
              Id.class,
              BsonType.INT32,
              IntValue.class,
              BsonType.INT64,
              LongValue.class));

  private final Map<BsonType, Class<?>> map;

  public BsonTypeValueClassMap(Map<BsonType, Class<?>> map) {
    map.values().stream()
        .filter(it -> !Value.class.isAssignableFrom(it))
        .findAny()
        .ifPresent(
            it -> {
              throw new IllegalArgumentException(it + "is not a subclass of Value");
            });
    this.map = map;
  }

  public Class<? extends Value> get(BsonType bsonType) {
    return (Class<? extends Value>) map.get(bsonType);
  }
}
