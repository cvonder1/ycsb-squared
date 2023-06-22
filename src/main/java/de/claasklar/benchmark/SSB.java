package de.claasklar.benchmark;

import static de.claasklar.primitives.document.ArrayValue.array;
import static de.claasklar.primitives.document.IntValue.integer;
import static de.claasklar.primitives.document.NestedObjectValue.object;
import static de.claasklar.primitives.document.NullValue.nill;
import static de.claasklar.primitives.document.StringValue.string;

import com.mongodb.ConnectionString;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import de.claasklar.database.mongodb.MongoDatabaseBuilder;
import de.claasklar.generation.ContextDocumentGeneratorBuilder;
import de.claasklar.generation.ContextlessDocumentGeneratorBuilder;
import de.claasklar.generation.SameAggregationGenerator;
import de.claasklar.generation.inserters.FixedKeyObjectInserter;
import de.claasklar.generation.inserters.ObjectInserter;
import de.claasklar.idStore.InMemoryIdStore;
import de.claasklar.phase.IndexPhase;
import de.claasklar.phase.LoadPhase;
import de.claasklar.phase.PowerTestTransactionPhase;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.BoolValue;
import de.claasklar.primitives.document.IntValue;
import de.claasklar.primitives.document.LongValue;
import de.claasklar.primitives.document.NestedObjectValue;
import de.claasklar.primitives.document.NullValue;
import de.claasklar.primitives.document.ObjectValue;
import de.claasklar.primitives.document.StringValue;
import de.claasklar.primitives.document.Value;
import de.claasklar.primitives.index.IndexConfiguration;
import de.claasklar.primitives.query.AggregationOptions;
import de.claasklar.random.distribution.RandomNumberGenerator;
import de.claasklar.random.distribution.StdRandomNumberGenerator;
import de.claasklar.random.distribution.document.ExistingDocumentDistribution;
import de.claasklar.random.distribution.document.SimpleDocumentDistribution;
import de.claasklar.random.distribution.id.UniformIdDistribution;
import de.claasklar.random.distribution.reference.ConstantNumberReferencesDistribution;
import de.claasklar.random.distribution.reference.ReferencesDistribution;
import de.claasklar.specification.PrimaryWriteSpecification;
import de.claasklar.specification.ReadSpecification;
import de.claasklar.specification.WriteSpecification;
import de.claasklar.specification.WriteSpecificationRegistry;
import de.claasklar.util.Pair;
import de.claasklar.util.TelemetryConfig;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.extension.incubator.metrics.ExtendedLongHistogramBuilder;
import java.time.Clock;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Month;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class SSB {

  private static final double V_STR_LOW = 0.4;
  private static final double V_STR_HIGH = 1.6;
  private static final int S_ADDR_LEN = 15;
  private static final int C_ADDR_LEN = 15;
  private static final int P_MFG_MIN = 1;
  private static final int P_MFG_MAX = 5;
  private static final int P_CAT_MIN = 1;
  private static final int P_CAT_MAX = 5;
  private static final int P_BRAND_MIN = 1;
  private static final int P_BRAND_MAX = 40;
  private static final int P_SIZE_MIN = 1;
  private static final int P_SIZE_MAX = 50;
  private static final int O_LCNT_MIN = 1;
  private static final int O_LCNT_MAX = 7;
  private static final LocalDate D_START_DATE =
      LocalDate.ofInstant(Instant.ofEpochSecond(694245661), ZoneOffset.UTC);
  private static final LocalDate D_END_DATE = D_START_DATE.plus(7, ChronoUnit.YEARS);
  private static final int L_QTY_MIN = 1;
  private static final int L_QTY_MAX = 50;
  private static final int L_DCNT_MIN = 0;
  private static final int L_DCNT_MAX = 10;
  private static final int L_TAX_MIN = 1;
  private static final int L_TAX_MAX = 7;
  private static final int L_PRICE_MAX = 20000;
  private static final long PENNIES = 100;
  private static final long L_BASE_PRICE = 90000;
  private static final long O_TOTAL_MIN = L_BASE_PRICE;
  private static final long O_TOTAL_MAX =
      O_LCNT_MAX * (L_BASE_PRICE + L_PRICE_MAX + (L_PRICE_MAX - 1) % 1000 * 100);
  private static final Pair[] nations = {
    new Pair<>("ALGERIA", "AFRICA"),
    new Pair<>("ARGENTINA", "AMERICA"),
    new Pair<>("BRAZIL", "AMERICA"),
    new Pair<>("CANADA", "AMERICA"),
    new Pair<>("EGYPT", "MIDDLE EAST"),
    new Pair<>("ETHIOPIA", "AFRICA"),
    new Pair<>("FRANCE", "EUROPE"),
    new Pair<>("GERMANY", "EUROPE"),
    new Pair<>("INDIA", "ASIA"),
    new Pair<>("INDONESIA", "ASIA"),
    new Pair<>("IRAN", "MIDDLE EAST"),
    new Pair<>("IRAQ", "MIDDLE EAST"),
    new Pair<>("JAPAN", "ASIA"),
    new Pair<>("JORDAN", "MIDDLE EAST"),
    new Pair<>("KENYA", "AFRICA"),
    new Pair<>("MOROCCO", "AFRICA"),
    new Pair<>("MOZAMBIQUE", "AFRICA"),
    new Pair<>("PERU", "AMERICA"),
    new Pair<>("CHINA", "ASIA"),
    new Pair<>("ROMANIA", "EUROPE"),
    new Pair<>("SAUDI ARABIA", "MIDDLE EAST"),
    new Pair<>("VIETNAM", "ASIA"),
    new Pair<>("RUSSIA", "ASIA"),
    new Pair<>("UNITED KINGDOM", "EUROPE"),
    new Pair<>("UNITED STATES", "AMERICA"),
  };

  private static final String[] colors =
      new String[] {
        "almond",
        "antique",
        "aquamarine",
        "azure",
        "beige",
        "bisque",
        "black",
        "blanched",
        "blue",
        "blush",
        "brown",
        "burlywood",
        "burnished",
        "chartreuse",
        "chiffon",
        "chocolate",
        "coral",
        "cornflower",
        "cornsilk",
        "cream",
        "cyan",
        "dark",
        "deep",
        "dim",
        "dodger",
        "drab",
        "firebrick",
        "floral",
        "forest",
        "frosted",
        "gainsboro",
        "ghost",
        "goldenrod",
        "green",
        "grey",
        "honeydew",
        "hot",
        "indian",
        "ivory",
        "khaki",
        "lace",
        "lavender",
        "lawn",
        "lemon",
        "light",
        "lime",
        "linen",
        "magenta",
        "maroon",
        "medium",
        "metallic",
        "midnight",
        "mint",
        "misty",
        "moccasin",
        "navajo",
        "navy",
        "olive",
        "orange",
        "orchid",
        "pale",
        "papaya",
        "peach",
        "peru",
        "pink",
        "plum",
        "powder",
        "puff",
        "purple",
        "red",
        "rose",
        "rosy",
        "royal",
        "saddle",
        "salmon",
        "sandy",
        "seashell",
        "sienna",
        "sky",
        "slate",
        "smoke",
        "snow",
        "spring",
        "steel",
        "tan",
        "thistle",
        "tomato",
        "turquoise",
        "violet",
        "wheat",
        "white",
        "yellow"
      };

  private static final String[] segments =
      new String[] {"AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"};
  private static final String[] types =
      new String[] {
        "STANDARD ANODIZED TIN",
        "STANDARD ANODIZED NICKEL",
        "STANDARD ANODIZED BRASS",
        "STANDARD ANODIZED STEEL",
        "STANDARD ANODIZED COPPER",
        "STANDARD BURNISHED TIN",
        "STANDARD BURNISHED NICKEL",
        "STANDARD BURNISHED BRASS",
        "STANDARD BURNISHED STEEL",
        "STANDARD BURNISHED COPPER",
        "STANDARD PLATED TIN",
        "STANDARD PLATED NICKEL",
        "STANDARD PLATED BRASS",
        "STANDARD PLATED STEEL",
        "STANDARD PLATED COPPER",
        "STANDARD POLISHED TIN",
        "STANDARD POLISHED NICKEL",
        "STANDARD POLISHED BRASS",
        "STANDARD POLISHED STEEL",
        "STANDARD POLISHED COPPER",
        "STANDARD BRUSHED TIN",
        "STANDARD BRUSHED NICKEL",
        "STANDARD BRUSHED BRASS",
        "STANDARD BRUSHED STEEL",
        "STANDARD BRUSHED COPPER",
        "SMALL ANODIZED TIN",
        "SMALL ANODIZED NICKEL",
        "SMALL ANODIZED BRASS",
        "SMALL ANODIZED STEEL",
        "SMALL ANODIZED COPPER",
        "SMALL BURNISHED TIN",
        "SMALL BURNISHED NICKEL",
        "SMALL BURNISHED BRASS",
        "SMALL BURNISHED STEEL",
        "SMALL BURNISHED COPPER",
        "SMALL PLATED TIN",
        "SMALL PLATED NICKEL",
        "SMALL PLATED BRASS",
        "SMALL PLATED STEEL",
        "SMALL PLATED COPPER",
        "SMALL POLISHED TIN",
        "SMALL POLISHED NICKEL",
        "SMALL POLISHED BRASS",
        "SMALL POLISHED STEEL",
        "SMALL POLISHED COPPER",
        "SMALL BRUSHED TIN",
        "SMALL BRUSHED NICKEL",
        "SMALL BRUSHED BRASS",
        "SMALL BRUSHED STEEL",
        "SMALL BRUSHED COPPER",
        "MEDIUM ANODIZED TIN",
        "MEDIUM ANODIZED NICKEL",
        "MEDIUM ANODIZED BRASS",
        "MEDIUM ANODIZED STEEL",
        "MEDIUM ANODIZED COPPER",
        "MEDIUM BURNISHED TIN",
        "MEDIUM BURNISHED NICKEL",
        "MEDIUM BURNISHED BRASS",
        "MEDIUM BURNISHED STEEL",
        "MEDIUM BURNISHED COPPER",
        "MEDIUM PLATED TIN",
        "MEDIUM PLATED NICKEL",
        "MEDIUM PLATED BRASS",
        "MEDIUM PLATED STEEL",
        "MEDIUM PLATED COPPER",
        "MEDIUM POLISHED TIN",
        "MEDIUM POLISHED NICKEL",
        "MEDIUM POLISHED BRASS",
        "MEDIUM POLISHED STEEL",
        "MEDIUM POLISHED COPPER",
        "MEDIUM BRUSHED TIN",
        "MEDIUM BRUSHED NICKEL",
        "MEDIUM BRUSHED BRASS",
        "MEDIUM BRUSHED STEEL",
        "MEDIUM BRUSHED COPPER",
        "LARGE ANODIZED TIN",
        "LARGE ANODIZED NICKEL",
        "LARGE ANODIZED BRASS",
        "LARGE ANODIZED STEEL",
        "LARGE ANODIZED COPPER",
        "LARGE BURNISHED TIN",
        "LARGE BURNISHED NICKEL",
        "LARGE BURNISHED BRASS",
        "LARGE BURNISHED STEEL",
        "LARGE BURNISHED COPPER",
        "LARGE PLATED TIN",
        "LARGE PLATED NICKEL",
        "LARGE PLATED BRASS",
        "LARGE PLATED STEEL",
        "LARGE PLATED COPPER",
        "LARGE POLISHED TIN",
        "LARGE POLISHED NICKEL",
        "LARGE POLISHED BRASS",
        "LARGE POLISHED STEEL",
        "LARGE POLISHED COPPER",
        "LARGE BRUSHED TIN",
        "LARGE BRUSHED NICKEL",
        "LARGE BRUSHED BRASS",
        "LARGE BRUSHED STEEL",
        "LARGE BRUSHED COPPER",
        "ECONOMY ANODIZED TIN",
        "ECONOMY ANODIZED NICKEL",
        "ECONOMY ANODIZED BRASS",
        "ECONOMY ANODIZED STEEL",
        "ECONOMY ANODIZED COPPER",
        "ECONOMY BURNISHED TIN",
        "ECONOMY BURNISHED NICKEL",
        "ECONOMY BURNISHED BRASS",
        "ECONOMY BURNISHED STEEL",
        "ECONOMY BURNISHED COPPER",
        "ECONOMY PLATED TIN",
        "ECONOMY PLATED NICKEL",
        "ECONOMY PLATED BRASS",
        "ECONOMY PLATED STEEL",
        "ECONOMY PLATED COPPER",
        "ECONOMY POLISHED TIN",
        "ECONOMY POLISHED NICKEL",
        "ECONOMY POLISHED BRASS",
        "ECONOMY POLISHED STEEL",
        "ECONOMY POLISHED COPPER",
        "ECONOMY BRUSHED TIN",
        "ECONOMY BRUSHED NICKEL",
        "ECONOMY BRUSHED BRASS",
        "ECONOMY BRUSHED STEEL",
        "ECONOMY BRUSHED COPPER",
        "PROMO ANODIZED TIN",
        "PROMO ANODIZED NICKEL",
        "PROMO ANODIZED BRASS",
        "PROMO ANODIZED STEEL",
        "PROMO ANODIZED COPPER",
        "PROMO BURNISHED TIN",
        "PROMO BURNISHED NICKEL",
        "PROMO BURNISHED BRASS",
        "PROMO BURNISHED STEEL",
        "PROMO BURNISHED COPPER",
        "PROMO PLATED TIN",
        "PROMO PLATED NICKEL",
        "PROMO PLATED BRASS",
        "PROMO PLATED STEEL",
        "PROMO PLATED COPPER",
        "PROMO POLISHED TIN",
        "PROMO POLISHED NICKEL",
        "PROMO POLISHED BRASS",
        "PROMO POLISHED STEEL",
        "PROMO POLISHED COPPER",
        "PROMO BRUSHED TIN",
        "PROMO BRUSHED NICKEL",
        "PROMO BRUSHED BRASS",
        "PROMO BRUSHED STEEL",
        "PROMO BRUSHED COPPER"
      };

  private static final String[] containers =
      new String[] {
        "SM CASE",
        "SM BOX",
        "SM BAG",
        "SM JAR",
        "SM PACK",
        "SM PKG",
        "SM CAN",
        "SM DRUM",
        "LG CASE",
        "LG BOX",
        "LG BAG",
        "LG JAR",
        "LG PACK",
        "LG PKG",
        "LG CAN",
        "LG DRUM",
        "MED CASE",
        "MED BOX",
        "MED BAG",
        "MED JAR",
        "MED PACK",
        "MED PKG",
        "MED CAN",
        "MED DRUM",
        "JUMBO CASE",
        "JUMBO BOX",
        "JUMBO BAG",
        "JUMBO JAR",
        "JUMBO PACK",
        "JUMBO PKG",
        "JUMBO CAN",
        "JUMBO DRUM",
        "WRAP CASE",
        "WRAP BOX",
        "WRAP BAG",
        "WRAP JAR",
        "WRAP PACK",
        "WRAP PKG",
        "WRAP CAN",
        "WRAP DRUM"
      };

  private static final Season[] seasons =
      new Season[] {
        new Season(
            "Christmas",
            LocalDate.of(1980, Month.NOVEMBER, 1),
            LocalDate.of(1980, Month.DECEMBER, 31)),
        new Season(
            "Summer", LocalDate.of(1980, Month.MAY, 1), LocalDate.of(1980, Month.AUGUST, 31)),
        new Season(
            "Winter", LocalDate.of(1980, Month.JANUARY, 1), LocalDate.of(1980, Month.MARCH, 31)),
        new Season(
            "Fall", LocalDate.of(1980, Month.SEPTEMBER, 1), LocalDate.of(1980, Month.OCTOBER, 31))
      };

  private static final LocalDate[] holidays =
      new LocalDate[] {
        LocalDate.of(1980, Month.DECEMBER, 24),
        LocalDate.of(1980, Month.JANUARY, 1),
        LocalDate.of(1980, Month.FEBRUARY, 20),
        LocalDate.of(1980, Month.APRIL, 20),
        LocalDate.of(1980, Month.JULY, 20),
        LocalDate.of(1980, Month.JUNE, 20),
        LocalDate.of(1980, Month.AUGUST, 20),
        LocalDate.of(1980, Month.SEPTEMBER, 20),
        LocalDate.of(1980, Month.OCTOBER, 20),
        LocalDate.of(1980, Month.NOVEMBER, 20)
      };

  private static final String[] priorities =
      new String[] {"1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"};

  private static final String[] shipModes =
      new String[] {"REG AIR", "AIR", "RAIL", "TRUCK", "MAIL", "FOB", "SHIP"};

  // Is there a difference in embedding documents from another collection instead of generating the
  // document at random
  public static Pair<Benchmark, Span> createSSBDenormalized(int scaleFactor) {
    var openTelemetry = TelemetryConfig.buildOpenTelemetry();
    var threadExecutor = Executors.newVirtualThreadPerTaskExecutor();

    var transactionDurationHistogram =
        ((ExtendedLongHistogramBuilder)
                openTelemetry
                    .meterBuilder(TelemetryConfig.METRIC_SCOPE_NAME)
                    .setInstrumentationVersion(TelemetryConfig.version())
                    .build()
                    .histogramBuilder("transaction_duration")
                    .ofLongs())
            .setAdvice(
                advice -> advice.setExplicitBucketBoundaries(TelemetryConfig.bucketBoundaries()))
            .setUnit("ms")
            .setDescription(
                "Tracks duration of transactions across all specifications. Attributes give more detail about collection and operation.")
            .build();
    var tracer =
        openTelemetry.getTracer(
            TelemetryConfig.INSTRUMENTATION_SCOPE_NAME, TelemetryConfig.version());
    var applicationSpan = tracer.spanBuilder(TelemetryConfig.APPLICATION_SPAN_NAME).startSpan();

    var random = new StdRandomNumberGenerator();

    var idStore = new InMemoryIdStore();
    var allCollections =
        List.of(
            new CollectionName("customers"),
            new CollectionName("parts"),
            new CollectionName("suppliers"),
            new CollectionName("dates"),
            new CollectionName("lineOrders"));
    var database =
        MongoDatabaseBuilder.builder()
            .databaseName(TelemetryConfig.version())
            .connectionString(new ConnectionString("mongodb://mongodb"))
            .openTelemetry(openTelemetry)
            .tracer(tracer)
            .collections(allCollections)
            .databaseReadConcern(ReadConcern.DEFAULT)
            .databaseWriteConcern(WriteConcern.JOURNALED)
            .databaseReadPreference(ReadPreference.nearest())
            .build();
    var registry = new WriteSpecificationRegistry();

    var supplierDocumentGenerator =
        ContextlessDocumentGeneratorBuilder.builder()
            .field(
                "name",
                s ->
                    s.prefixedRandomString(
                        "Supplier", () -> Long.toString(random.nextLong(0, 999999999999999999L))))
            .field(
                "address",
                s ->
                    s.ssbRandomLengthString(
                        (int) (S_ADDR_LEN * V_STR_LOW), (int) (S_ADDR_LEN * V_STR_HIGH) + 1))
            .field(nationAndPhoneInserter(random))
            .build();
    var supplierWriteSpecification =
        new WriteSpecification(
            new CollectionName("suppliers"),
            supplierDocumentGenerator,
            new ReferencesDistribution[0],
            database,
            idStore,
            threadExecutor,
            transactionDurationHistogram,
            tracer,
            Clock.systemUTC());
    registry.register(supplierWriteSpecification);

    var customerDocumentGenerator =
        ContextlessDocumentGeneratorBuilder.builder()
            .field(
                "name",
                s ->
                    s.prefixedRandomString(
                        "Customer", () -> Long.toString(random.nextLong(0, 999999999999999999L))))
            .field(
                "address",
                s ->
                    s.ssbRandomLengthString(
                        (int) (C_ADDR_LEN * V_STR_LOW), (int) (C_ADDR_LEN * V_STR_HIGH) + 1))
            .field(nationAndPhoneInserter(random))
            .field("mktsegment", s -> s.uniformSelection(segments))
            .build();
    var customerWriteSpecification =
        new WriteSpecification(
            new CollectionName("customers"),
            customerDocumentGenerator,
            new ReferencesDistribution[0],
            database,
            idStore,
            threadExecutor,
            transactionDurationHistogram,
            tracer,
            Clock.systemUTC());
    registry.register(customerWriteSpecification);

    var partDocumentGenerator =
        ContextlessDocumentGeneratorBuilder.builder()
            .field("name", s -> s.selectNonRepeating(colors, 2, ((s1, s2) -> s1 + " " + s2)))
            .field("color", s -> s.uniformSelection(colors))
            .field(
                (it) -> {
                  var mfgr = "MFGR#" + random.nextInt(P_MFG_MIN, P_MFG_MAX + 1);
                  var cat = mfgr + random.nextInt(P_CAT_MIN, P_CAT_MAX + 1);
                  var brand = cat + random.nextInt(P_BRAND_MIN, P_BRAND_MAX + 1);
                  new FixedKeyObjectInserter("mfgr", () -> new StringValue(mfgr))
                      .andThen(new FixedKeyObjectInserter("category", () -> new StringValue(cat)))
                      .andThen(new FixedKeyObjectInserter("brand1", () -> new StringValue(brand)))
                      .accept(it);
                })
            .field("type", s -> s.uniformSelection(types))
            .field("size", s -> s.uniformIntSupplier(P_SIZE_MIN, P_SIZE_MAX + 1))
            .field("container", s -> s.uniformSelection(containers))
            .build();
    var partWriteSpecification =
        new WriteSpecification(
            new CollectionName("parts"),
            partDocumentGenerator,
            new ReferencesDistribution[0],
            database,
            idStore,
            threadExecutor,
            transactionDurationHistogram,
            tracer,
            Clock.systemUTC());
    registry.register(partWriteSpecification);

    var dateDocumentGenerator =
        ContextlessDocumentGeneratorBuilder.builder()
            .field(
                new ObjectInserter() {
                  private final DateTimeFormatter dateTimeFormatter =
                      DateTimeFormatter.ofPattern("MMMM d, yyyy");
                  private final AtomicLong state = new AtomicLong(0);

                  @Override
                  public void accept(ObjectValue objectValue) {
                    var currentState = state.getAndIncrement();
                    var nextDate = D_START_DATE.plus(currentState, ChronoUnit.DAYS);

                    objectValue.put("date", new StringValue(nextDate.format(dateTimeFormatter)));
                    objectValue.put("dayofweek", new StringValue(nextDate.getDayOfWeek().name()));
                    objectValue.put("month", new StringValue(nextDate.getMonth().name()));
                    objectValue.put("year", new IntValue(nextDate.getYear()));
                    objectValue.put(
                        "yearmonthnum",
                        new IntValue(nextDate.getYear() * 100 + nextDate.getMonth().ordinal() + 1));
                    objectValue.put(
                        "yearmonth",
                        new StringValue(
                            (nextDate.getMonth().name() + " ").substring(0, 3)
                                + nextDate.getYear()));
                    objectValue.put(
                        "daynuminweek",
                        new IntValue((nextDate.getDayOfWeek().ordinal() + 1) % 7 + 1));
                    objectValue.put("daynuminmonth", new IntValue(nextDate.getDayOfMonth()));
                    objectValue.put("daynuminyear", new IntValue(nextDate.getDayOfYear()));
                    objectValue.put("monthnuminyear", new IntValue(nextDate.getMonthValue()));
                    objectValue.put("weeknuminyear", new IntValue(nextDate.getDayOfYear() / 7 + 1));
                    objectValue.put("sellingseason", genSeason(nextDate));
                    objectValue.put(
                        "lastdayinweekfl",
                        new BoolValue(nextDate.getDayOfWeek() == DayOfWeek.SATURDAY));
                    objectValue.put("lastdayinmonthfl", new BoolValue(isLastDayOfMonth(nextDate)));
                    objectValue.put("holidayfl", new BoolValue(isHoliday(nextDate)));
                    objectValue.put(
                        "weekdayfl", new BoolValue(nextDate.getDayOfWeek().ordinal() < 6));
                  }
                })
            .build();
    var dateWriteSpecification =
        new WriteSpecification(
            new CollectionName("dates"),
            dateDocumentGenerator,
            new ReferencesDistribution[0],
            database,
            idStore,
            threadExecutor,
            transactionDurationHistogram,
            tracer,
            Clock.systemUTC());
    registry.register(dateWriteSpecification);

    var lineOrderDocumentGenerator =
        ContextDocumentGeneratorBuilder.builder()
            .field("linenumber", s -> s.uniformIntSupplier(O_LCNT_MIN, O_LCNT_MAX))
            .fieldFromPipe(
                "custkey",
                f ->
                    f.selectCollection(
                        new CollectionName("customers"), c -> c.selectByPath("$.[0]._id").toId()))
            .fieldFromPipe(
                "partkey",
                f ->
                    f.selectCollection(
                        new CollectionName("parts"), c -> c.selectByPath("$.[0]._id").toId()))
            .fieldFromPipe(
                "suppkey",
                f ->
                    f.selectCollection(
                        new CollectionName("suppliers"), c -> c.selectByPath("$.[0]._id").toId()))
            .fieldFromPipe(
                "orderdate",
                f ->
                    f.selectCollection(
                        new CollectionName("dates"), c -> c.selectByPath("$.[0]._id").toId()))
            .field("orderpriority", s -> s.uniformSelection(priorities))
            .field("shippriority", s -> s.fixedString("0"))
            .field(
                (object) -> {
                  var quantity = random.nextLong(L_QTY_MIN, L_QTY_MAX + 1);
                  var rprice = rpbRoutine(random);
                  var extendedPrice = rprice * quantity;
                  var discount = random.nextLong(L_DCNT_MIN, L_DCNT_MAX + 1);
                  var revenue = extendedPrice * (100 - discount) / PENNIES;
                  long supplyCost = 6 * rprice / 10;

                  object.put("quantity", new LongValue(quantity));
                  object.put("extendedprice", new LongValue(extendedPrice));
                  object.put("discount", new LongValue(discount));
                  object.put("revenue", new LongValue(revenue));
                  object.put("supplycost", new LongValue(supplyCost));
                })
            .field("ordtotalprice", s -> s.uniformLongSupplier(O_TOTAL_MIN, O_TOTAL_MAX))
            .field("tax", s -> s.uniformIntSupplier(L_TAX_MIN, L_TAX_MAX + 1))
            .fieldFromPipe(
                "commitdate",
                f ->
                    f.selectCollection(
                        new CollectionName("dates"), c -> c.selectByPath("$.[1]._id").toId()))
            .field("shipmode", s -> s.uniformSelection(shipModes))
            .build();
    var lineOrderWriteSpecification =
        new PrimaryWriteSpecification(
            new CollectionName("lineOrders"),
            new ReferencesDistribution[] {
              new ConstantNumberReferencesDistribution(
                  1,
                  new SimpleDocumentDistribution(
                      new CollectionName("customers"),
                      new UniformIdDistribution(scaleFactor * 30_000L, random),
                      idStore,
                      database,
                      registry,
                      tracer),
                  threadExecutor),
              new ConstantNumberReferencesDistribution(
                  1,
                  new SimpleDocumentDistribution(
                      new CollectionName("parts"),
                      new UniformIdDistribution(
                          200_000
                              * ((long) Math.floor(1 + (Math.log(scaleFactor) / Math.log(2.0)))),
                          random),
                      idStore,
                      database,
                      registry,
                      tracer),
                  threadExecutor),
              new ConstantNumberReferencesDistribution(
                  1,
                  new ExistingDocumentDistribution(
                      50,
                      new SimpleDocumentDistribution(
                          new CollectionName("suppliers"),
                          new UniformIdDistribution(scaleFactor * 2_000L, random),
                          idStore,
                          database,
                          registry,
                          tracer),
                      database,
                      Executors.newFixedThreadPool(1),
                      tracer),
                  threadExecutor),
              new ConstantNumberReferencesDistribution(
                  2,
                  new ExistingDocumentDistribution(
                      50,
                      new SimpleDocumentDistribution(
                          new CollectionName("dates"),
                          new UniformIdDistribution(
                              D_START_DATE.until(D_END_DATE, ChronoUnit.DAYS), random),
                          idStore,
                          database,
                          registry,
                          tracer),
                      database,
                      Executors.newFixedThreadPool(1),
                      tracer),
                  threadExecutor)
            },
            lineOrderDocumentGenerator,
            database,
            threadExecutor,
            transactionDurationHistogram,
            idStore,
            tracer,
            Clock.systemUTC());

    var q1_1Generator =
        new SameAggregationGenerator(
            new CollectionName("lineOrders"),
            AggregationOptions.aggregate("lineOrders")
                .pipeline(
                    List.of(
                        object(
                            "$match",
                            object(
                                "discount",
                                object("$gte", integer(1), "$lte", integer(3)),
                                "quantity",
                                object("$lte", integer(25)))),
                        lookupOrderDate(),
                        object("$match", object("orderdate.year", integer(1993))),
                        revenue())));
    var q1_1 =
        new ReadSpecification(
            "q1.1",
            q1_1Generator,
            database,
            transactionDurationHistogram,
            tracer,
            Clock.systemUTC());

    var q1_2Generator =
        new SameAggregationGenerator(
            new CollectionName("lineOrders"),
            AggregationOptions.aggregate("lineOrders")
                .pipeline(
                    List.of(
                        object(
                            "$match",
                            object(
                                "discount",
                                object("$gte", integer(4), "$lte", integer(6)),
                                "quantity",
                                object("$gte", integer(26), "$lte", integer(35)))),
                        lookupOrderDate(),
                        object("$match", object("orderdate.yearmonthnum", integer(199401))),
                        revenue())));
    var q1_2 =
        new ReadSpecification(
            "q1.2",
            q1_2Generator,
            database,
            transactionDurationHistogram,
            tracer,
            Clock.systemUTC());

    var q1_3Generator =
        new SameAggregationGenerator(
            new CollectionName("lineOrders"),
            AggregationOptions.aggregate("lineOrders")
                .pipeline(
                    List.of(
                        object(
                            "$match",
                            object(
                                "discount",
                                object("$gte", integer(5), "$lte", integer(7)),
                                "quantity",
                                object("$gte", integer(26), "$lte", integer(35)))),
                        lookupOrderDate(),
                        object(
                            "$match",
                            object(
                                "orderdate.weeknuminyear",
                                integer(6),
                                "orderdate.year",
                                integer(1994))),
                        revenue())));
    var q1_3 =
        new ReadSpecification(
            "q1.3",
            q1_3Generator,
            database,
            transactionDurationHistogram,
            tracer,
            Clock.systemUTC());

    var q2_1Generator =
        new SameAggregationGenerator(
            new CollectionName("lineOrders"),
            AggregationOptions.aggregate("lineOrders")
                .pipeline(
                    List.of(
                        lookupPart(),
                        object("$match", object("part.category", string("MFGR#12"))),
                        lookupSupplier(),
                        object("$match", object("supplier.region", string("AMERICA"))),
                        lookupOrderDate(),
                        object(
                            "$group",
                            object(
                                "_id",
                                object(
                                    "d_year",
                                    string("$orderdate.year"),
                                    "p_brand",
                                    string("$part.brand1")),
                                "total_revenue",
                                object("$sum", string("$revenue")))),
                        object("$sort", object("_id.p_brand", integer(1))),
                        object("$sort", object("_id.d_year", integer(1))))));
    var q2_1 =
        new ReadSpecification(
            "q2.1",
            q2_1Generator,
            database,
            transactionDurationHistogram,
            tracer,
            Clock.systemUTC());

    var q2_2Generator =
        new SameAggregationGenerator(
            new CollectionName("lineOrders"),
            AggregationOptions.aggregate("lineOrders")
                .pipeline(
                    List.of(
                        lookupPart(),
                        object(
                            "$match",
                            object(
                                "part.brand1",
                                object(
                                    "$in",
                                    array(
                                        string("MFGR#2221"),
                                        string("MFGR#2222"),
                                        string("MFGR#2223"),
                                        string("MFGR#2224"),
                                        string("MFGR#2225"),
                                        string("MFGR#2226"),
                                        string("MFGR#2227"),
                                        string("MFGR#2228"))))),
                        lookupSupplier(),
                        object("$match", object("supplier.region", string("ASIA"))),
                        lookupOrderDate(),
                        object(
                            "$group",
                            object(
                                "_id",
                                object(
                                    "d_year",
                                    string("$orderdate.year"),
                                    "p_brand",
                                    string("$part.brand1")),
                                "total_revenue",
                                object("$sum", string("$revenue")))),
                        object("$sort", object("_id.p_brand", integer(1))),
                        object("$sort", object("_id.d_year", integer(1))))));

    var q2_2 =
        new ReadSpecification(
            "q2.2",
            q2_2Generator,
            database,
            transactionDurationHistogram,
            tracer,
            Clock.systemUTC());

    var q2_3Generator =
        new SameAggregationGenerator(
            new CollectionName("lineOrders"),
            AggregationOptions.aggregate("lineOrders")
                .pipeline(
                    List.of(
                        lookupPart(),
                        object("$match", object("part.brand1", string("MFGR#2221"))),
                        lookupSupplier(),
                        object("$match", object("supplier.region", string("EUROPE"))),
                        lookupOrderDate(),
                        object(
                            "$group",
                            object(
                                "_id",
                                object(
                                    "d_year",
                                    string("$orderdate.year"),
                                    "p_brand",
                                    string("$part.brand1")),
                                "total_revenue",
                                object("$sum", string("$revenue")))),
                        object("$sort", object("_id.p_brand", integer(1))),
                        object("$sort", object("_id.d_year", integer(1))))));
    var q2_3 =
        new ReadSpecification(
            "q2.3",
            q2_3Generator,
            database,
            transactionDurationHistogram,
            tracer,
            Clock.systemUTC());

    var q3_1Generator =
        new SameAggregationGenerator(
            new CollectionName("lineOrders"),
            AggregationOptions.aggregate("lineOrders")
                .pipeline(
                    List.of(
                        lookupCustomer(),
                        object("$match", object("customer.region", string("ASIA"))),
                        lookupSupplier(),
                        object("$match", object("supplier.region", string("ASIA"))),
                        lookupOrderDate(),
                        object(
                            "$match",
                            object(
                                "orderdate.year",
                                object("$gte", integer(1992), "$lte", integer(1997)))),
                        object(
                            "$group",
                            object(
                                "_id",
                                object(
                                    Map.of(
                                        "c_nation",
                                        string("$customer.nation"),
                                        "s_nation",
                                        string("$supplier.nation"),
                                        "d_year",
                                        string("$orderdate.year"))),
                                "revenue",
                                object("$sum", string("$revenue")))),
                        object("$sort", object("d_year", integer(1), "revenue", integer(-1))))));
    var q3_1 =
        new ReadSpecification(
            "q3.1",
            q3_1Generator,
            database,
            transactionDurationHistogram,
            tracer,
            Clock.systemUTC());

    var q3_2Generator =
        new SameAggregationGenerator(
            new CollectionName("lineOrders"),
            AggregationOptions.aggregate("lineOrders")
                .pipeline(
                    List.of(
                        lookupCustomer(),
                        object("$match", object("customer.nation", string("UNITED STATES"))),
                        lookupSupplier(),
                        object("$match", object("supplier.nation", string("UNITED STATES"))),
                        lookupOrderDate(),
                        object(
                            "$match",
                            object(
                                "orderdate.year",
                                object("$gte", integer(1992), "$lte", integer(1997)))),
                        object(
                            "$group",
                            object(
                                "_id",
                                object(
                                    Map.of(
                                        "c_city",
                                        string("$customer.city"),
                                        "s_city",
                                        string("$supplier.city"),
                                        "d_year",
                                        string("$orderdate.year"))),
                                "revenue",
                                object("$sum", string("$revenue")))),
                        object("$sort", object("d_year", integer(1), "revenue", integer(-1))))));
    var q3_2 =
        new ReadSpecification(
            "q3.2",
            q3_2Generator,
            database,
            transactionDurationHistogram,
            tracer,
            Clock.systemUTC());

    var q3_3Generator =
        new SameAggregationGenerator(
            new CollectionName("lineOrders"),
            AggregationOptions.aggregate("lineOrders")
                .pipeline(
                    List.of(
                        lookupCustomer(),
                        object(
                            "$match",
                            object(
                                "customer.city",
                                object("$in", array(string("UNITED KI1"), string("UNITED KI5"))))),
                        lookupSupplier(),
                        object(
                            "$match",
                            object(
                                "supplier.city",
                                object("$in", array(string("UNITED KI1"), string("UNITED KI5"))))),
                        lookupOrderDate(),
                        object(
                            "$match",
                            object(
                                "orderdate.year",
                                object("$gte", integer(1992), "$lte", integer(1997)))),
                        object(
                            "$group",
                            object(
                                "_id",
                                object(
                                    Map.of(
                                        "c_city",
                                        string("$customer.city"),
                                        "s_city",
                                        string("$supplier.city"),
                                        "d_year",
                                        string("$orderdate.year"))),
                                "revenue",
                                object("$sum", string("$revenue")))),
                        object("$sort", object("d_year", integer(1), "revenue", integer(-1))))));
    var q3_3 =
        new ReadSpecification(
            "q3.3",
            q3_3Generator,
            database,
            transactionDurationHistogram,
            tracer,
            Clock.systemUTC());

    var q3_4Generator =
        new SameAggregationGenerator(
            new CollectionName("lineOrders"),
            AggregationOptions.aggregate("lineOrders")
                .pipeline(
                    List.of(
                        lookupCustomer(),
                        object(
                            "$match",
                            object(
                                "customer.city",
                                object("$in", array(string("UNITED KI1"), string("UNITED KI5"))))),
                        lookupSupplier(),
                        object(
                            "$match",
                            object(
                                "supplier.city",
                                object("$in", array(string("UNITED KI1"), string("UNITED KI5"))))),
                        lookupOrderDate(),
                        object("$match", object("orderdate.yearmonth", string("DEC1997"))),
                        object(
                            "$group",
                            object(
                                "_id",
                                object(
                                    Map.of(
                                        "c_city",
                                        string("$customer.city"),
                                        "s_city",
                                        string("$supplier.city"),
                                        "d_year",
                                        string("$orderdate.year"))),
                                "revenue",
                                object("$sum", string("$revenue")))),
                        object("$sort", object("d_year", integer(1), "revenue", integer(-1))))));
    var q3_4 =
        new ReadSpecification(
            "q3.4",
            q3_4Generator,
            database,
            transactionDurationHistogram,
            tracer,
            Clock.systemUTC());

    var q4_1Generator =
        new SameAggregationGenerator(
            new CollectionName("lineOrders"),
            AggregationOptions.aggregate("lineOrders")
                .pipeline(
                    List.of(
                        lookupCustomer(),
                        object("$match", object("customer.region", string("AMERICA"))),
                        lookupSupplier(),
                        object("$match", object("supplier.region", string("AMERICA"))),
                        lookupPart(),
                        object(
                            "$match",
                            object(
                                "part.mfgr",
                                object("$in", array(string("MFGR#1"), string("MFGR#2"))))),
                        lookupOrderDate(),
                        object(
                            "$group",
                            object(
                                Map.of(
                                    "_id",
                                    object(
                                        "d_year",
                                        string("$orderdate.year"),
                                        "c_nation",
                                        string("$customer.nation")),
                                    "totalrevenue",
                                    object("$sum", string("$revenue")),
                                    "totalsupplycost",
                                    object("$sum", string("$supplycost"))))),
                        object(
                            "$addFields",
                            object(
                                "profit",
                                object(
                                    "$subtract",
                                    array(string("$totalrevenue"), string("$totalsupplycost"))))),
                        object("$sort", object("d_year", integer(1), "c_nation", integer(1))))));
    var q4_1 =
        new ReadSpecification(
            "q4.1",
            q4_1Generator,
            database,
            transactionDurationHistogram,
            tracer,
            Clock.systemUTC());

    var q4_2Generator =
        new SameAggregationGenerator(
            new CollectionName("lineOrders"),
            AggregationOptions.aggregate("lineOrders")
                .pipeline(
                    List.of(
                        lookupOrderDate(),
                        object(
                            "$match",
                            object(
                                "orderdate.year",
                                object("$in", array(integer(1997), integer(1998))))),
                        lookupCustomer(),
                        object("$match", object("customer.region", string("AMERICA"))),
                        lookupSupplier(),
                        object("$match", object("supplier.region", string("AMERICA"))),
                        lookupPart(),
                        object(
                            "$match",
                            object(
                                "part.mfgr",
                                object("$in", array(string("MFGR#1"), string("MFGR#2"))))),
                        object(
                            "$group",
                            object(
                                Map.of(
                                    "_id",
                                    object(
                                        "d_year",
                                        string("$orderdate.year"),
                                        "c_nation",
                                        string("$customer.nation")),
                                    "totalrevenue",
                                    object("$sum", string("$revenue")),
                                    "totalsupplycost",
                                    object("$sum", string("$supplycost"))))),
                        object(
                            "$addFields",
                            object(
                                "profit",
                                object(
                                    "$subtract",
                                    array(string("$totalrevenue"), string("$totalsupplycost"))))),
                        object("$sort", object("d_year", integer(1), "c_nation", integer(1))))));
    var q4_2 =
        new ReadSpecification(
            "q4.2",
            q4_2Generator,
            database,
            transactionDurationHistogram,
            tracer,
            Clock.systemUTC());

    var q4_3Generator =
        new SameAggregationGenerator(
            new CollectionName("lineOrders"),
            AggregationOptions.aggregate("lineOrders")
                .pipeline(
                    List.of(
                        lookupOrderDate(),
                        object(
                            "$match",
                            object(
                                "orderdate.year",
                                object("$in", array(integer(1997), integer(1998))))),
                        lookupCustomer(),
                        object("$match", object("customer.region", string("AMERICA"))),
                        lookupSupplier(),
                        object("$match", object("supplier.nation", string("UNITED STATES"))),
                        lookupPart(),
                        object("$match", object("part.category", string("MFGR#14"))),
                        object(
                            "$group",
                            object(
                                Map.of(
                                    "_id",
                                    object(
                                        Map.of(
                                            "d_year",
                                            string("$orderdate.year"),
                                            "s_city",
                                            string("$supplier.city"),
                                            "p_brand1",
                                            string("$part.brand1"))),
                                    "totalrevenue",
                                    object("$sum", string("$revenue")),
                                    "totalsupplycost",
                                    object("$sum", string("$supplycost"))))),
                        object(
                            "$addFields",
                            object(
                                "profit",
                                object(
                                    "$subtract",
                                    array(string("$totalrevenue"), string("$totalsupplycost"))))),
                        object(
                            "$sort",
                            object(
                                Map.of(
                                    "d_year",
                                    integer(1),
                                    "s_city",
                                    integer(1),
                                    "p_brand1",
                                    integer(1)))))));
    var q4_3 =
        new ReadSpecification(
            "q4.3",
            q4_3Generator,
            database,
            transactionDurationHistogram,
            tracer,
            Clock.systemUTC());

    return new Pair<>(
        new Benchmark(
            new IndexPhase(new IndexConfiguration[0], database, applicationSpan, tracer),
            new LoadPhase(
                lineOrderWriteSpecification,
                scaleFactor * 6_000_000L,
                100,
                applicationSpan,
                tracer),
            new PowerTestTransactionPhase(
                List.of(
                    q1_1, q1_2, q1_3, q2_1, q2_2, q2_3, q3_1, q3_2, q3_3, q3_4, q4_1, q4_2, q4_3),
                applicationSpan,
                tracer),
            database,
            List.of(threadExecutor),
            applicationSpan),
        applicationSpan);
  }

  public static Benchmark createSSBEmbedded(long scaleFactor) {}

  private static ObjectInserter nationAndPhoneInserter(RandomNumberGenerator random) {
    return (it) -> {
      var nationI = random.nextInt(0, nations.length);
      var nationRegion = nations[nationI];
      var nationInserter =
          new FixedKeyObjectInserter(
              "nation", () -> new StringValue((String) nationRegion.first()));
      var regionInserter =
          new FixedKeyObjectInserter(
              "region", () -> new StringValue((String) nationRegion.second()));
      var cityInserter =
          new FixedKeyObjectInserter(
              "city", () -> new StringValue(genCity((String) nationRegion.first(), random)));
      var phoneInserter =
          new FixedKeyObjectInserter("phone", () -> new StringValue(genPhone(nationI, random)));
      nationInserter
          .andThen(regionInserter)
          .andThen(cityInserter)
          .andThen(phoneInserter)
          .accept(it);
    };
  }

  private static String genCity(String nation, RandomNumberGenerator random) {
    var num = random.nextInt(0, 10);
    if (nation.length() > 9) {
      nation = nation.substring(0, 9);
    }
    return String.format("%1$9s%2$d", nation, num);
  }

  private static String genPhone(int nation, RandomNumberGenerator random) {
    int acode = random.nextInt(100, 999);
    int exchg = random.nextInt(100, 999);
    int number = random.nextInt(1000, 9999);
    return String.format(
        "%02d-%03d-%03d-%04d", 10 + (nation % nations.length), acode, exchg, number);
  }

  private static Value genSeason(LocalDate date) {
    for (var season : seasons) {
      if (season.isDatePartOf(date)) {
        return new StringValue(season.name);
      }
    }
    return NullValue.VALUE;
  }

  private static boolean isLastDayOfMonth(LocalDate date) {
    var yearMonth = YearMonth.of(date.getYear(), date.getMonth());
    return yearMonth.atEndOfMonth().isEqual(date);
  }

  private static boolean isHoliday(LocalDate date) {
    for (var holiday : holidays) {
      if (holiday.withYear(date.getYear()).isEqual(date)) {
        return true;
      }
    }
    return false;
  }

  private static long rpbRoutine(RandomNumberGenerator random) {
    var price = L_BASE_PRICE;
    var p = random.nextLong(0, L_PRICE_MAX + 1);
    price += p;
    price += (p % 1000) * 100;
    return price;
  }

  private static NestedObjectValue lookupOrderDate() {
    return object(
        "$lookup",
        object(
            Map.of(
                "from",
                string("dates"),
                "localField",
                string("orderdate"),
                "foreignField",
                string("_id"),
                "as",
                string("orderdate"))));
  }

  private static NestedObjectValue revenue() {
    return object(
        "$group",
        object(
            "_id",
            nill(),
            "revenue",
            object(
                "$sum",
                object("$multiply", array(string("$extendedprice"), string("$discount"))))));
  }

  private static NestedObjectValue lookupPart() {
    return object(
        "$lookup",
        object(
            Map.of(
                "from", string("parts"),
                "localField", string("partkey"),
                "foreignField", string("_id"),
                "as", string("part"))));
  }

  private static NestedObjectValue lookupSupplier() {
    return object(
        "$lookup",
        object(
            Map.of(
                "from", string("suppliers"),
                "localField", string("suppkey"),
                "foreignField", string("_id"),
                "as", string("supplier"))));
  }

  private static NestedObjectValue lookupCustomer() {
    return object(
        "$lookup",
        object(
            Map.of(
                "from", string("customers"),
                "localField", string("custkey"),
                "foreignField", string("_id"),
                "as", string("customer"))));
  }

  private static class Season {
    LocalDate start;
    LocalDate end;
    String name;

    public Season(String name, LocalDate start, LocalDate end) {
      this.start = start;
      this.end = end;
      this.name = name;
    }

    public boolean isDatePartOf(LocalDate date) {
      var adjustedStart = start.withYear(date.getYear());
      var adjustedEnd = end.withYear(date.getYear());
      return date.isAfter(adjustedStart) && date.isBefore(adjustedEnd);
    }
  }
}
