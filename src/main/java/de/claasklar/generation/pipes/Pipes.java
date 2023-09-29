package de.claasklar.generation.pipes;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Predicate;
import com.jayway.jsonpath.internal.ParseContextImpl;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.ArrayValue;
import de.claasklar.primitives.document.BoolValue;
import de.claasklar.primitives.document.ByteValue;
import de.claasklar.primitives.document.DoubleValue;
import de.claasklar.primitives.document.Id;
import de.claasklar.primitives.document.IntValue;
import de.claasklar.primitives.document.LongValue;
import de.claasklar.primitives.document.NestedObjectValue;
import de.claasklar.primitives.document.NullValue;
import de.claasklar.primitives.document.ObjectValue;
import de.claasklar.primitives.document.OurDocument;
import de.claasklar.primitives.document.StringValue;
import de.claasklar.primitives.document.Value;
import java.awt.geom.Arc2D.Float;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Pipes {

  private static final Logger logger = LoggerFactory.getLogger(Pipes.class);

  public static PipeBuilder selectCollection(CollectionName collectionName) {
    return new PipeBuilder(collectionName);
  }

  public static class PipeBuilder {

    private final ParseContextImpl parseContext = new ParseContextImpl();

    private Pipe<Map<CollectionName, OurDocument[]>, ?> pipe;

    public PipeBuilder(CollectionName collectionName) {
      this.pipe = (input) -> input.get(collectionName);
    }

    /**
     * Select from the incoming array/object by an JsonPath expression.
     *
     * <p>Input: OurDocument[] <br>
     * Output: Object, mostly List&lt;Object&gt; or Map&lt;String, Object&gt;
     *
     * @see JsonPath#compile(String, Predicate...)
     * @param path JsonPath expression
     * @return {@link Object}
     */
    public PipeBuilder selectByPath(String path) {
      var jsonPath = JsonPath.compile(path);
      this.pipe =
          pipe.pipe(
                  (input) -> {
                    var collection =
                        Arrays.stream((OurDocument[]) input)
                            .map(ObjectValue::toBasicType)
                            .collect(Collectors.toList());
                    return parseContext.parse(collection);
                  })
              .pipe(input -> input.read(jsonPath));
      return this;
    }

    /**
     * Convert incoming objects to {@link ArrayValue}.
     *
     * <p>Input: List&lt;Object&gt; or Object[] Output: {@link ArrayValue}
     *
     * @return Pipe with ArrayValue output
     */
    public Pipe<Map<CollectionName, OurDocument[]>, ArrayValue> toArray() {
      return this.pipe
          .pipe(
              input -> {
                if (input instanceof List) {
                  return ((List) input).stream();
                } else if (input instanceof Object[]) {
                  return Arrays.stream((Object[]) input);
                } else {
                  throw new IllegalArgumentException(input + " is of unknown type");
                }
              })
          .pipe(
              input -> {
                var arrayValue = new ArrayValue();
                input.forEach(it -> arrayValue.add(toValue(it)));
                return arrayValue;
              });
    }

    /**
     * Convert incoming Object into {@link ObjectValue}.
     *
     * <p>Input: Map&lt;String, Object&gt; <br>
     * Output: ObjectValue
     *
     * @return Pipe with ObjectValue output
     */
    public Pipe<Map<CollectionName, OurDocument[]>, ObjectValue> toObject() {
      return this.pipe
          .pipe(input -> (Map) input)
          .pipe(
              input -> {
                if (input != null) {
                  return (NestedObjectValue) toValue(input);
                } else {
                  return NullValue.VALUE;
                }
              });
    }

    /**
     * Convert incoming byte[] to {@link Id}.
     *
     * <p>Incoming: byte[] with length 1 <br>
     * Output: Id
     *
     * @return Pipe with Id output
     */
    public Pipe<Map<CollectionName, OurDocument[]>, Id> toId() {
      return this.pipe.pipe(input -> (byte[]) input).pipe(Id::new);
    }

    private Value toValue(Object o) {
      logger.atTrace().log(() -> "mapping class " + o.getClass());
      if (o == null) {
        return NullValue.VALUE;
      } else if (o instanceof List) {
        var value = new ArrayValue();
        ((List<Object>) o).stream().map(it -> toValue(it)).forEach(it -> value.add(it));
        return value;
      } else if (o instanceof Map) {
        var value = new NestedObjectValue();
        ((Map<String, Object>) o)
            .entrySet()
            .forEach(it -> value.put(it.getKey(), toValue(it.getValue())));
        return value;
      } else if (o instanceof OurDocument) {
        return (OurDocument) o;
      } else if (o instanceof byte[]) {
        var value = (byte[]) o;
        if (value.length == 12) {
          return new Id(value);
        } else {
          return new ByteValue(value);
        }
      } else if (o instanceof String) {
        return new StringValue((String) o);
      } else if (o instanceof Float || o instanceof Double) {
        return new DoubleValue((float) o);
      } else if (o instanceof Boolean) {
        return new BoolValue((boolean) o);
      } else if (o instanceof Integer) {
        return new IntValue((int) o);
      } else if (o instanceof Long) {
        return new LongValue((long) o);
      } else {
        throw new IllegalArgumentException(o + " cannot be converted");
      }
    }

    public Pipe<Map<CollectionName, OurDocument[]>, Object> build() {
      return (Pipe<Map<CollectionName, OurDocument[]>, Object>) pipe;
    }
  }
}
