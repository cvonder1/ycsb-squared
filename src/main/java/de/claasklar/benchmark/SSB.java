package de.claasklar.benchmark;

import static de.claasklar.primitives.document.ArrayValue.array;
import static de.claasklar.primitives.document.IntValue.integer;
import static de.claasklar.primitives.document.NestedObjectValue.object;
import static de.claasklar.primitives.document.NullValue.nill;
import static de.claasklar.primitives.document.StringValue.string;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import de.claasklar.generation.ContextDocumentGeneratorBuilder;
import de.claasklar.generation.ContextlessDocumentGeneratorBuilder;
import de.claasklar.generation.SameAggregationGenerator;
import de.claasklar.generation.inserters.FixedKeyObjectInserter;
import de.claasklar.generation.inserters.ObjectInserter;
import de.claasklar.generation.pipes.Pipes.PipeBuilder;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.*;
import de.claasklar.primitives.document.BoolValue;
import de.claasklar.primitives.document.IntValue;
import de.claasklar.primitives.document.LongValue;
import de.claasklar.primitives.document.NestedObjectValue;
import de.claasklar.primitives.document.NullValue;
import de.claasklar.primitives.document.ObjectValue;
import de.claasklar.primitives.document.StringValue;
import de.claasklar.primitives.document.Value;
import de.claasklar.primitives.query.AggregationOptions;
import de.claasklar.random.distribution.RandomNumberGenerator;
import de.claasklar.random.distribution.StdRandomNumberGenerator;
import de.claasklar.util.Pair;
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
import java.util.function.Consumer;

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

  private static final RandomNumberGenerator random = new StdRandomNumberGenerator();

  public static Benchmark createSSBDenormalized(int scaleFactor) {
    return BenchmarkBuilder.builder()
        .database(databaseConfig())
        .writeSpecification(writeSuppliersConfig())
        .writeSpecification(writeCustomersConfig())
        .writeSpecification(writePartsConfig())
        .writeSpecification(writeDatesConfig())
        .primaryWriteSpecification("write_lineOrders", writeLineOrdersConfig(scaleFactor))
        .readSpecification(queryQ1_1Config())
        .readSpecification(queryQ1_2Config())
        .readSpecification(queryQ1_3Config())
        .readSpecification(queryQ2_1Config())
        .readSpecification(queryQ2_2Config())
        .readSpecification(queryQ2_3Config())
        .readSpecification(queryQ3_1Config())
        .readSpecification(queryQ3_2Config())
        .readSpecification(queryQ3_3Config())
        .readSpecification(queryQ3_4Config())
        .readSpecification(queryQ4_1Config())
        .readSpecification(queryQ4_2Config())
        .readSpecification(queryQ4_3Config())
        .loadPhase(loadPhaseConfig(scaleFactor))
        .transactionPhase(transactionPhaseConfig())
        .build();
  }

  public static Benchmark createSSBEmbedded(long scaleFactor) {
    return BenchmarkBuilder.builder()
        .database(databaseConfig())
        .writeSpecification(writeSuppliersConfig())
        .writeSpecification(writeCustomersConfig())
        .writeSpecification(writePartsConfig())
        .writeSpecification(writeDatesConfig())
        .primaryWriteSpecification("write_lineOrders", writeLineOrdersEmbeddedConfig(scaleFactor))
        .readSpecification(queryQ1_1EmbeddedConfig())
        .readSpecification(queryQ1_2EmbeddedConfig())
        .readSpecification(queryQ1_3EmbeddedConfig())
        .readSpecification(queryQ2_1EmbeddedConfig())
        .readSpecification(queryQ2_2EmbeddedConfig())
        .readSpecification(queryQ2_3EmbeddedConfig())
        .readSpecification(queryQ3_1EmbeddedConfig())
        .readSpecification(queryQ3_2EmbeddedConfig())
        .readSpecification(queryQ3_3EmbeddedConfig())
        .readSpecification(queryQ3_4EmbeddedConfig())
        .readSpecification(queryQ4_1EmbeddedConfig())
        .readSpecification(queryQ4_2EmbeddedConfig())
        .readSpecification(queryQ4_3EmbeddedConfig())
        .loadPhase(loadPhaseConfig(scaleFactor))
        .transactionPhase(transactionPhaseConfig())
        .build();
  }

  private static Consumer<BenchmarkBuilder.MongoConfiguration> databaseConfig() {
    return config ->
        config
            .databaseReadConcern(ReadConcern.DEFAULT)
            .databaseWriteConcern(WriteConcern.JOURNALED)
            .databaseReadPreference(ReadPreference.nearest());
  }

  private static Consumer<BenchmarkBuilder.TransactionPhaseConfig> transactionPhaseConfig() {
    return transactionPhaseConfig ->
        transactionPhaseConfig.powerTest(
            powerTestConfig -> {
              for (int i = 0; i < 100; i++) {
                powerTestConfig.specification(
                    "q1.1", "q1.2", "q1.3", "q2.1", "q2.2", "q2.3", "q3.1", "q3.2", "q3.3", "q3.4",
                    "q4.1", "q4.2", "q4.3");
              }
            });
  }

  private static Consumer<BenchmarkBuilder.LoadPhaseConfig> loadPhaseConfig(long scaleFactor) {
    return loadPhaseConfig ->
        loadPhaseConfig
            .primaryWriteSpecification(scaleFactor * 6_000_000L, "write_lineOrders")
            .numThreads(100);
  }

  private static Consumer<BenchmarkBuilder.ReadSpecificationConfig> queryQ4_3Config() {
    return readConfig ->
        readConfig
            .name("q4.3")
            .queryGenerator(
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
                                object(
                                    "$match", object("supplier.nation", string("UNITED STATES"))),
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
                                            array(
                                                string("$totalrevenue"),
                                                string("$totalsupplycost"))))),
                                object(
                                    "$sort",
                                    object(
                                        Map.of(
                                            "d_year",
                                            integer(1),
                                            "s_city",
                                            integer(1),
                                            "p_brand1",
                                            integer(1))))))));
  }

  private static Consumer<BenchmarkBuilder.ReadSpecificationConfig> queryQ4_2Config() {
    return readConfig ->
        readConfig
            .name("q4.2")
            .queryGenerator(
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
                                            array(
                                                string("$totalrevenue"),
                                                string("$totalsupplycost"))))),
                                object(
                                    "$sort",
                                    object("d_year", integer(1), "c_nation", integer(1)))))));
  }

  private static Consumer<BenchmarkBuilder.ReadSpecificationConfig> queryQ4_1Config() {
    return readConfig ->
        readConfig
            .name("q4.1")
            .queryGenerator(
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
                                            array(
                                                string("$totalrevenue"),
                                                string("$totalsupplycost"))))),
                                object(
                                    "$sort",
                                    object("d_year", integer(1), "c_nation", integer(1)))))));
  }

  private static Consumer<BenchmarkBuilder.ReadSpecificationConfig> queryQ3_4Config() {
    return readConfig ->
        readConfig
            .name("q3.4")
            .queryGenerator(
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
                                        object(
                                            "$in",
                                            array(string("UNITED KI1"), string("UNITED KI5"))))),
                                lookupSupplier(),
                                object(
                                    "$match",
                                    object(
                                        "supplier.city",
                                        object(
                                            "$in",
                                            array(string("UNITED KI1"), string("UNITED KI5"))))),
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
                                object(
                                    "$sort",
                                    object("d_year", integer(1), "revenue", integer(-1)))))));
  }

  private static Consumer<BenchmarkBuilder.ReadSpecificationConfig> queryQ3_3Config() {
    return readConfig ->
        readConfig
            .name("q3.3")
            .queryGenerator(
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
                                        object(
                                            "$in",
                                            array(string("UNITED KI1"), string("UNITED KI5"))))),
                                lookupSupplier(),
                                object(
                                    "$match",
                                    object(
                                        "supplier.city",
                                        object(
                                            "$in",
                                            array(string("UNITED KI1"), string("UNITED KI5"))))),
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
                                object(
                                    "$sort",
                                    object("d_year", integer(1), "revenue", integer(-1)))))));
  }

  private static Consumer<BenchmarkBuilder.ReadSpecificationConfig> queryQ3_2Config() {
    return readConfig ->
        readConfig
            .name("q3.2")
            .queryGenerator(
                new SameAggregationGenerator(
                    new CollectionName("lineOrders"),
                    AggregationOptions.aggregate("lineOrders")
                        .pipeline(
                            List.of(
                                lookupCustomer(),
                                object(
                                    "$match", object("customer.nation", string("UNITED STATES"))),
                                lookupSupplier(),
                                object(
                                    "$match", object("supplier.nation", string("UNITED STATES"))),
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
                                object(
                                    "$sort",
                                    object("d_year", integer(1), "revenue", integer(-1)))))));
  }

  private static Consumer<BenchmarkBuilder.ReadSpecificationConfig> queryQ3_1Config() {
    return readConfig ->
        readConfig
            .name("q3.1")
            .queryGenerator(
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
                                object(
                                    "$sort",
                                    object("d_year", integer(1), "revenue", integer(-1)))))));
  }

  private static Consumer<BenchmarkBuilder.ReadSpecificationConfig> queryQ2_3Config() {
    return readConfig ->
        readConfig
            .name("q2.3")
            .queryGenerator(
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
                                object("$sort", object("_id.d_year", integer(1)))))));
  }

  private static Consumer<BenchmarkBuilder.ReadSpecificationConfig> queryQ2_2Config() {
    return readConfig ->
        readConfig
            .name("q2.2")
            .queryGenerator(
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
                                object("$sort", object("_id.d_year", integer(1)))))));
  }

  private static Consumer<BenchmarkBuilder.ReadSpecificationConfig> queryQ2_1Config() {
    return readConfig ->
        readConfig
            .name("q2.1")
            .queryGenerator(
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
                                object("$sort", object("_id.d_year", integer(1)))))));
  }

  private static Consumer<BenchmarkBuilder.ReadSpecificationConfig> queryQ1_3Config() {
    return readConfig ->
        readConfig
            .name("q1.3")
            .queryGenerator(
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
                                revenue()))));
  }

  private static Consumer<BenchmarkBuilder.ReadSpecificationConfig> queryQ1_2Config() {
    return readConfig ->
        readConfig
            .name("q1.2")
            .queryGenerator(
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
                                revenue()))));
  }

  private static Consumer<BenchmarkBuilder.ReadSpecificationConfig> queryQ1_1Config() {
    return readConfig ->
        readConfig
            .name("q1.1")
            .queryGenerator(
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
                                revenue()))));
  }

  private static Consumer<BenchmarkBuilder.PrimaryWriteSpecificationConfig> writeLineOrdersConfig(
      int scaleFactor) {
    return config ->
        config
            .collectionName("lineOrders")
            .documentGenerator(
                ContextDocumentGeneratorBuilder.builder()
                    .field("linenumber", s -> s.uniformIntSupplier(O_LCNT_MIN, O_LCNT_MAX))
                    .fieldFromPipe(
                        "custkey",
                        f ->
                            f.selectCollection(
                                new CollectionName("customers"),
                                c -> c.selectByPath("$.[0]._id").toId()))
                    .fieldFromPipe(
                        "partkey",
                        f ->
                            f.selectCollection(
                                new CollectionName("parts"),
                                c -> c.selectByPath("$.[0]._id").toId()))
                    .fieldFromPipe(
                        "suppkey",
                        f ->
                            f.selectCollection(
                                new CollectionName("suppliers"),
                                c -> c.selectByPath("$.[0]._id").toId()))
                    .fieldFromPipe(
                        "orderdate",
                        f ->
                            f.selectCollection(
                                new CollectionName("dates"),
                                c -> c.selectByPath("$.[0]._id").toId()))
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
                                new CollectionName("dates"),
                                c -> c.selectByPath("$.[1]._id").toId()))
                    .field("shipmode", s -> s.uniformSelection(shipModes))
                    .build())
            .referenceDistributionConfig(
                referenceConfig ->
                    referenceConfig
                        .constantNumber(1)
                        .documentDistribution(
                            documentDistributionConfig ->
                                documentDistributionConfig
                                    .collectionName("customers")
                                    .idDistribution(f -> f.uniform(scaleFactor * 30_000L))
                                    .existing()))
            .referenceDistributionConfig(
                referenceConfig ->
                    referenceConfig
                        .constantNumber(1)
                        .documentDistribution(
                            documentDistributionConfig ->
                                documentDistributionConfig
                                    .collectionName("parts")
                                    .idDistribution(
                                        f ->
                                            f.uniform(
                                                200_000
                                                    * ((long)
                                                        Math.floor(
                                                            1
                                                                + (Math.log(scaleFactor)
                                                                    / Math.log(2.0))))))))
            .referenceDistributionConfig(
                referencesDistributionConfig ->
                    referencesDistributionConfig
                        .constantNumber(1)
                        .documentDistribution(
                            documentDistributionConfig ->
                                documentDistributionConfig
                                    .collectionName("suppliers")
                                    .idDistribution(f -> f.uniform(scaleFactor * 2_000L))
                                    .existing()
                                    .bufferSize(100)
                                    .executorService(Executors.newFixedThreadPool(1))))
            .referenceDistributionConfig(
                referencesDistributionConfig ->
                    referencesDistributionConfig
                        .constantNumber(2)
                        .documentDistribution(
                            documentDistributionConfig ->
                                documentDistributionConfig
                                    .collectionName("dates")
                                    .idDistribution(
                                        f ->
                                            f.uniform(
                                                D_START_DATE.until(D_END_DATE, ChronoUnit.DAYS)))
                                    .existing()
                                    .executorService(Executors.newFixedThreadPool(1))
                                    .bufferSize(100)));
  }

  private static Consumer<BenchmarkBuilder.DocumentGenerationSpecificationConfig>
      writeDatesConfig() {
    return config ->
        config
            .collectionName("dates")
            .documentGenerator(
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

                            objectValue.put(
                                "date", new StringValue(nextDate.format(dateTimeFormatter)));
                            objectValue.put(
                                "dayofweek", new StringValue(nextDate.getDayOfWeek().name()));
                            objectValue.put("month", new StringValue(nextDate.getMonth().name()));
                            objectValue.put("year", new IntValue(nextDate.getYear()));
                            objectValue.put(
                                "yearmonthnum",
                                new IntValue(
                                    nextDate.getYear() * 100 + nextDate.getMonth().ordinal() + 1));
                            objectValue.put(
                                "yearmonth",
                                new StringValue(
                                    (nextDate.getMonth().name() + " ").substring(0, 3)
                                        + nextDate.getYear()));
                            objectValue.put(
                                "daynuminweek",
                                new IntValue((nextDate.getDayOfWeek().ordinal() + 1) % 7 + 1));
                            objectValue.put(
                                "daynuminmonth", new IntValue(nextDate.getDayOfMonth()));
                            objectValue.put("daynuminyear", new IntValue(nextDate.getDayOfYear()));
                            objectValue.put(
                                "monthnuminyear", new IntValue(nextDate.getMonthValue()));
                            objectValue.put(
                                "weeknuminyear", new IntValue(nextDate.getDayOfYear() / 7 + 1));
                            objectValue.put("sellingseason", genSeason(nextDate));
                            objectValue.put(
                                "lastdayinweekfl",
                                new BoolValue(nextDate.getDayOfWeek() == DayOfWeek.SATURDAY));
                            objectValue.put(
                                "lastdayinmonthfl", new BoolValue(isLastDayOfMonth(nextDate)));
                            objectValue.put("holidayfl", new BoolValue(isHoliday(nextDate)));
                            objectValue.put(
                                "weekdayfl", new BoolValue(nextDate.getDayOfWeek().ordinal() < 6));
                          }
                        })
                    .build());
  }

  private static Consumer<BenchmarkBuilder.DocumentGenerationSpecificationConfig>
      writePartsConfig() {
    return config ->
        config
            .collectionName("parts")
            .documentGenerator(
                ContextlessDocumentGeneratorBuilder.builder()
                    .field(
                        "name", s -> s.selectNonRepeating(colors, 2, ((s1, s2) -> s1 + " " + s2)))
                    .field("color", s -> s.uniformSelection(colors))
                    .field(
                        (it) -> {
                          var mfgr = "MFGR#" + random.nextInt(P_MFG_MIN, P_MFG_MAX + 1);
                          var cat = mfgr + random.nextInt(P_CAT_MIN, P_CAT_MAX + 1);
                          var brand = cat + random.nextInt(P_BRAND_MIN, P_BRAND_MAX + 1);
                          new FixedKeyObjectInserter("mfgr", () -> new StringValue(mfgr))
                              .andThen(
                                  new FixedKeyObjectInserter(
                                      "category", () -> new StringValue(cat)))
                              .andThen(
                                  new FixedKeyObjectInserter(
                                      "brand1", () -> new StringValue(brand)))
                              .accept(it);
                        })
                    .field("type", s -> s.uniformSelection(types))
                    .field("size", s -> s.uniformIntSupplier(P_SIZE_MIN, P_SIZE_MAX + 1))
                    .field("container", s -> s.uniformSelection(containers))
                    .build());
  }

  private static Consumer<BenchmarkBuilder.DocumentGenerationSpecificationConfig>
      writeCustomersConfig() {
    return config ->
        config
            .collectionName("customers")
            .documentGenerator(
                ContextlessDocumentGeneratorBuilder.builder()
                    .field(
                        "name",
                        s ->
                            s.prefixedRandomString(
                                "Customer",
                                () -> Long.toString(random.nextLong(0, 999999999999999999L))))
                    .field(
                        "address",
                        s ->
                            s.ssbRandomLengthString(
                                (int) (C_ADDR_LEN * V_STR_LOW),
                                (int) (C_ADDR_LEN * V_STR_HIGH) + 1))
                    .field(nationAndPhoneInserter(random))
                    .field("mktsegment", s -> s.uniformSelection(segments))
                    .build());
  }

  private static Consumer<BenchmarkBuilder.DocumentGenerationSpecificationConfig>
      writeSuppliersConfig() {
    return config ->
        config
            .collectionName("suppliers")
            .documentGenerator(
                ContextlessDocumentGeneratorBuilder.builder()
                    .field(
                        "name",
                        s ->
                            s.prefixedRandomString(
                                "Supplier",
                                () -> Long.toString(random.nextLong(0, 999999999999999999L))))
                    .field(
                        "address",
                        s ->
                            s.ssbRandomLengthString(
                                (int) (S_ADDR_LEN * V_STR_LOW),
                                (int) (S_ADDR_LEN * V_STR_HIGH) + 1))
                    .field(nationAndPhoneInserter(random))
                    .build());
  }

  private static Consumer<BenchmarkBuilder.ReadSpecificationConfig> queryQ4_3EmbeddedConfig() {
    return readConfig ->
        readConfig
            .name("q4.3")
            .queryGenerator(
                new SameAggregationGenerator(
                    new CollectionName("lineOrders"),
                    AggregationOptions.aggregate("lineOrders")
                        .pipeline(
                            List.of(
                                object(
                                    "$match",
                                    object(
                                        "orderdate.year",
                                        object("$in", array(integer(1997), integer(1998))))),
                                object("$match", object("customer.region", string("AMERICA"))),
                                object(
                                    "$match", object("supplier.nation", string("UNITED STATES"))),
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
                                            array(
                                                string("$totalrevenue"),
                                                string("$totalsupplycost"))))),
                                object(
                                    "$sort",
                                    object(
                                        Map.of(
                                            "d_year",
                                            integer(1),
                                            "s_city",
                                            integer(1),
                                            "p_brand1",
                                            integer(1))))))));
  }

  private static Consumer<BenchmarkBuilder.ReadSpecificationConfig> queryQ4_2EmbeddedConfig() {
    return readConfig ->
        readConfig
            .name("q4.2")
            .queryGenerator(
                new SameAggregationGenerator(
                    new CollectionName("lineOrders"),
                    AggregationOptions.aggregate("lineOrders")
                        .pipeline(
                            List.of(
                                object(
                                    "$match",
                                    object(
                                        "orderdate.year",
                                        object("$in", array(integer(1997), integer(1998))))),
                                object("$match", object("customer.region", string("AMERICA"))),
                                object("$match", object("supplier.region", string("AMERICA"))),
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
                                            array(
                                                string("$totalrevenue"),
                                                string("$totalsupplycost"))))),
                                object(
                                    "$sort",
                                    object("d_year", integer(1), "c_nation", integer(1)))))));
  }

  private static Consumer<BenchmarkBuilder.ReadSpecificationConfig> queryQ4_1EmbeddedConfig() {
    return readConfig ->
        readConfig
            .name("q4.1")
            .queryGenerator(
                new SameAggregationGenerator(
                    new CollectionName("lineOrders"),
                    AggregationOptions.aggregate("lineOrders")
                        .pipeline(
                            List.of(
                                object("$match", object("customer.region", string("AMERICA"))),
                                object("$match", object("supplier.region", string("AMERICA"))),
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
                                            array(
                                                string("$totalrevenue"),
                                                string("$totalsupplycost"))))),
                                object(
                                    "$sort",
                                    object("d_year", integer(1), "c_nation", integer(1)))))));
  }

  private static Consumer<BenchmarkBuilder.ReadSpecificationConfig> queryQ3_4EmbeddedConfig() {
    return readConfig ->
        readConfig
            .name("q3.4")
            .queryGenerator(
                new SameAggregationGenerator(
                    new CollectionName("lineOrders"),
                    AggregationOptions.aggregate("lineOrders")
                        .pipeline(
                            List.of(
                                object(
                                    "$match",
                                    object(
                                        "customer.city",
                                        object(
                                            "$in",
                                            array(string("UNITED KI1"), string("UNITED KI5"))))),
                                object(
                                    "$match",
                                    object(
                                        "supplier.city",
                                        object(
                                            "$in",
                                            array(string("UNITED KI1"), string("UNITED KI5"))))),
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
                                object(
                                    "$sort",
                                    object("d_year", integer(1), "revenue", integer(-1)))))));
  }

  private static Consumer<BenchmarkBuilder.ReadSpecificationConfig> queryQ3_3EmbeddedConfig() {
    return readConfig ->
        readConfig
            .name("q3.3")
            .queryGenerator(
                new SameAggregationGenerator(
                    new CollectionName("lineOrders"),
                    AggregationOptions.aggregate("lineOrders")
                        .pipeline(
                            List.of(
                                object(
                                    "$match",
                                    object(
                                        "customer.city",
                                        object(
                                            "$in",
                                            array(string("UNITED KI1"), string("UNITED KI5"))))),
                                object(
                                    "$match",
                                    object(
                                        "supplier.city",
                                        object(
                                            "$in",
                                            array(string("UNITED KI1"), string("UNITED KI5"))))),
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
                                object(
                                    "$sort",
                                    object("d_year", integer(1), "revenue", integer(-1)))))));
  }

  private static Consumer<BenchmarkBuilder.ReadSpecificationConfig> queryQ3_2EmbeddedConfig() {
    return readConfig ->
        readConfig
            .name("q3.2")
            .queryGenerator(
                new SameAggregationGenerator(
                    new CollectionName("lineOrders"),
                    AggregationOptions.aggregate("lineOrders")
                        .pipeline(
                            List.of(
                                object(
                                    "$match", object("customer.nation", string("UNITED STATES"))),
                                object(
                                    "$match", object("supplier.nation", string("UNITED STATES"))),
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
                                object(
                                    "$sort",
                                    object("d_year", integer(1), "revenue", integer(-1)))))));
  }

  private static Consumer<BenchmarkBuilder.ReadSpecificationConfig> queryQ3_1EmbeddedConfig() {
    return readConfig ->
        readConfig
            .name("q3.1")
            .queryGenerator(
                new SameAggregationGenerator(
                    new CollectionName("lineOrders"),
                    AggregationOptions.aggregate("lineOrders")
                        .pipeline(
                            List.of(
                                object("$match", object("customer.region", string("ASIA"))),
                                object("$match", object("supplier.region", string("ASIA"))),
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
                                object(
                                    "$sort",
                                    object("d_year", integer(1), "revenue", integer(-1)))))));
  }

  private static Consumer<BenchmarkBuilder.ReadSpecificationConfig> queryQ2_3EmbeddedConfig() {
    return readConfig ->
        readConfig
            .name("q2.3")
            .queryGenerator(
                new SameAggregationGenerator(
                    new CollectionName("lineOrders"),
                    AggregationOptions.aggregate("lineOrders")
                        .pipeline(
                            List.of(
                                object("$match", object("part.brand1", string("MFGR#2221"))),
                                object("$match", object("supplier.region", string("EUROPE"))),
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
                                object("$sort", object("_id.d_year", integer(1)))))));
  }

  private static Consumer<BenchmarkBuilder.ReadSpecificationConfig> queryQ2_2EmbeddedConfig() {
    return readConfig ->
        readConfig
            .name("q2.2")
            .queryGenerator(
                new SameAggregationGenerator(
                    new CollectionName("lineOrders"),
                    AggregationOptions.aggregate("lineOrders")
                        .pipeline(
                            List.of(
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
                                object("$match", object("supplier.region", string("ASIA"))),
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
                                object("$sort", object("_id.d_year", integer(1)))))));
  }

  private static Consumer<BenchmarkBuilder.ReadSpecificationConfig> queryQ2_1EmbeddedConfig() {
    return readConfig ->
        readConfig
            .name("q2.1")
            .queryGenerator(
                new SameAggregationGenerator(
                    new CollectionName("lineOrders"),
                    AggregationOptions.aggregate("lineOrders")
                        .pipeline(
                            List.of(
                                object("$match", object("part.category", string("MFGR#12"))),
                                object("$match", object("supplier.region", string("AMERICA"))),
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
                                object("$sort", object("_id.d_year", integer(1)))))));
  }

  private static Consumer<BenchmarkBuilder.ReadSpecificationConfig> queryQ1_3EmbeddedConfig() {
    return readConfig ->
        readConfig
            .name("q1.3")
            .queryGenerator(
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
                                object(
                                    "$match",
                                    object(
                                        "orderdate.weeknuminyear",
                                        integer(6),
                                        "orderdate.year",
                                        integer(1994))),
                                revenue()))));
  }

  private static Consumer<BenchmarkBuilder.ReadSpecificationConfig> queryQ1_2EmbeddedConfig() {
    return readConfig ->
        readConfig
            .name("q1.2")
            .queryGenerator(
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
                                object("$match", object("orderdate.yearmonthnum", integer(199401))),
                                revenue()))));
  }

  private static Consumer<BenchmarkBuilder.ReadSpecificationConfig> queryQ1_1EmbeddedConfig() {
    return readConfig ->
        readConfig
            .name("q1.1")
            .queryGenerator(
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
                                object("$match", object("orderdate.year", integer(1993))),
                                revenue()))));
  }

  private static Consumer<BenchmarkBuilder.PrimaryWriteSpecificationConfig>
      writeLineOrdersEmbeddedConfig(long scaleFactor) {
    return writeConfig ->
        writeConfig
            .collectionName("lineOrders")
            .documentGenerator(
                ContextDocumentGeneratorBuilder.builder()
                    .field("linenumber", s -> s.uniformIntSupplier(O_LCNT_MIN, O_LCNT_MAX))
                    .fieldFromPipe(
                        "customer",
                        f ->
                            f.selectCollection(
                                new CollectionName("customers"), PipeBuilder::toObject))
                    .fieldFromPipe(
                        "part",
                        f -> f.selectCollection(new CollectionName("parts"), PipeBuilder::toObject))
                    .fieldFromPipe(
                        "supplier",
                        f ->
                            f.selectCollection(
                                new CollectionName("suppliers"), PipeBuilder::toObject))
                    .fieldFromPipe(
                        "orderdate",
                        f -> f.selectCollection(new CollectionName("dates"), PipeBuilder::toObject))
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
                        f -> f.selectCollection(new CollectionName("dates"), PipeBuilder::toObject))
                    .field("shipmode", s -> s.uniformSelection(shipModes))
                    .build())
            .referenceDistributionConfig(
                referenceConfig ->
                    referenceConfig
                        .constantNumber(1)
                        .documentDistribution(
                            documentDistributionConfig ->
                                documentDistributionConfig
                                    .collectionName("customers")
                                    .idDistribution(f -> f.uniform(scaleFactor * 30_000L))
                                    .recomputable()))
            .referenceDistributionConfig(
                referenceConfig ->
                    referenceConfig
                        .constantNumber(1)
                        .documentDistribution(
                            documentDistributionConfig ->
                                documentDistributionConfig
                                    .collectionName("parts")
                                    .idDistribution(
                                        f ->
                                            f.uniform(
                                                200_000
                                                    * ((long)
                                                        Math.floor(
                                                            1
                                                                + (Math.log(scaleFactor)
                                                                    / Math.log(2.0))))))
                                    .recomputable()))
            .referenceDistributionConfig(
                referencesDistributionConfig ->
                    referencesDistributionConfig
                        .constantNumber(1)
                        .documentDistribution(
                            documentDistributionConfig ->
                                documentDistributionConfig
                                    .collectionName("suppliers")
                                    .idDistribution(f -> f.uniform(scaleFactor * 2_000L))
                                    .recomputable()))
            .referenceDistributionConfig(
                referencesDistributionConfig ->
                    referencesDistributionConfig
                        .constantNumber(2)
                        .documentDistribution(
                            documentDistributionConfig ->
                                documentDistributionConfig
                                    .collectionName("dates")
                                    .idDistribution(
                                        f ->
                                            f.uniform(
                                                D_START_DATE.until(D_END_DATE, ChronoUnit.DAYS)))
                                    .recomputable()));
  }

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
