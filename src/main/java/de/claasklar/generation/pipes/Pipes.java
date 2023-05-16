package de.claasklar.generation.pipes;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.internal.ParseContextImpl;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.ArrayValue;
import de.claasklar.primitives.document.BoolValue;
import de.claasklar.primitives.document.ByteValue;
import de.claasklar.primitives.document.Document;
import de.claasklar.primitives.document.NestedObjectValue;
import de.claasklar.primitives.document.NumberValue;
import de.claasklar.primitives.document.ObjectValue;
import de.claasklar.primitives.document.StringValue;
import de.claasklar.primitives.document.Value;
import java.awt.geom.Arc2D.Float;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Pipes {

  public static PipeBuilder selectCollection(CollectionName collectionName) {
    return new PipeBuilder(collectionName);
  }

  public static class PipeBuilder {

    private final ParseContextImpl parseContext = new ParseContextImpl();

    private Pipe<Map<CollectionName, Document[]>, ?> pipe;

    public PipeBuilder(CollectionName collectionName) {
      this.pipe =
          (input) -> {
            var collection =
                Arrays.stream(input.get(collectionName))
                    .map(ObjectValue::toBasicType)
                    .collect(Collectors.toList());
            return parseContext.parse(collection);
          };
    }

    public PipeBuilder selectByPath(String path) {
      var jsonPath = JsonPath.compile(path);
      this.pipe =
          pipe.pipe(
                  (input) -> {
                    if (input instanceof ReadContext) {
                      return (ReadContext) input;
                    } else {
                      return parseContext.parse(input);
                    }
                  })
              .pipe(input -> input.read(jsonPath));
      return this;
    }

    public Pipe<Map<CollectionName, Document[]>, ArrayValue> toArray() {
      return this.pipe
          .pipe(input -> (List) input)
          .pipe(
              input -> {
                var arrayValue = new ArrayValue();
                input.stream().forEach(it -> arrayValue.add(toValue(it)));
                return arrayValue;
              });
    }

    public Pipe<Map<CollectionName, Document[]>, ObjectValue> toObject() {
      return this.pipe
          .pipe(input -> (Map<String, ?>) input)
          .pipe(input -> (ObjectValue) toValue(input));
    }

    private Value toValue(Object o) {
      if (o instanceof List) {
        var value = new ArrayValue();
        ((List<Object>) o).stream().map(it -> toValue(it)).forEach(it -> value.add(it));
        return value;
      } else if (o instanceof Map) {
        var value = new NestedObjectValue();
        ((Map<String, Object>) o)
            .entrySet()
            .forEach(it -> value.put(it.getKey(), toValue(it.getValue())));
        return value;
      } else if (o instanceof byte[]) {
        return new ByteValue((byte[]) o);
      } else if (o instanceof String) {
        return new StringValue((String) o);
      } else if (o instanceof Float) {
        return new NumberValue((float) o);
      } else if (o instanceof Boolean) {
        return new BoolValue((boolean) o);
      } else {
        throw new IllegalArgumentException(o.toString() + " cannot be converted");
      }
    }

    public Pipe<Map<CollectionName, Document[]>, Object> build() {
      return (Pipe<Map<CollectionName, Document[]>, Object>) pipe;
    }
  }
}
