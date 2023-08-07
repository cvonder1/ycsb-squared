package de.claasklar.idStore;

import static org.assertj.core.api.Assertions.assertThat;

import de.claasklar.primitives.CollectionName;
import java.util.LinkedList;
import java.util.Random;
import java.util.function.BiFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class InMemoryIdStoreTest {

  private InMemoryIdStore testSubject;

  @BeforeEach
  public void setup() {
    testSubject = new InMemoryIdStore();
  }

  @Test
  public void testStoreShouldStoreIdsSequentially() {
    // given
    var name = new CollectionName("test");
    // when
    for (int i = 0; i < 257; i++) {
      testSubject.store(name, i);
      int finalI = i;
      // then
      assertThat(testSubject.exists(name, i))
          .withFailMessage(() -> "could not find id " + finalI)
          .isTrue();
    }
    assertThat(testSubject.exists(name, 500)).isFalse();
  }

  @Test
  public void testStoreShouldStoreIdWithGaps() {
    // given
    var name = new CollectionName("test");
    // when
    testSubject.store(name, 788);
    // then
    assertThat(testSubject.exists(name, 788)).isTrue();
  }

  @Test
  public void testStoreIdsConcurrently() throws InterruptedException {
    // given
    var name = new CollectionName("test");
    BiFunction<Integer, Integer, Runnable> storingThread =
        (Integer min, Integer max) ->
            (Runnable)
                () -> {
                  for (int i = min; i < max; i++) {
                    testSubject.store(name, i);
                  }
                };
    var lowerNumbers = new Thread(storingThread.apply(0, 127));
    var upperNumbers = new Thread(storingThread.apply(127, 256));
    // when
    lowerNumbers.start();
    upperNumbers.start();
    lowerNumbers.join(1000);
    upperNumbers.join(1000);
    // then
    for (int i = 0; i < 256; i++) {
      int finalI = i;
      assertThat(testSubject.exists(name, i))
          .withFailMessage(() -> "could not find id " + finalI)
          .isTrue();
    }
  }

  @Test
  public void testExistsInParallel() throws Throwable {
    // given
    var name = new CollectionName("test");
    for (int i = 0; i < 100000; i++) {
      testSubject.store(name, i);
    }
    var random = new Random();
    var throwables = new LinkedList<Throwable>();
    // when
    var threads = new LinkedList<Thread>();
    for (int i = 0; i < 50; i++) {
      var thread =
          new Thread(
              () -> {
                for (int j = 0; j < 50000; j++) {
                  // then
                  assertThat(testSubject.exists(name, random.nextLong(100000))).isTrue();
                }
              });
      threads.add(thread);
    }
    for (int i = 0; i < 10; i++) {
      var finalI = i;
      var thread =
          new Thread(
              () -> {
                for (int j = 0; j < 1000; j++) {
                  testSubject.store(name, 100000 + 1000 * finalI + j);
                }
              });
      threads.add(thread);
    }
    threads.forEach(
        it -> it.setUncaughtExceptionHandler((thread1, throwable) -> throwables.add(throwable)));
    threads.forEach(Thread::start);
    threads.forEach(
        it -> {
          try {
            it.join(5000);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        });
    for (var throwable : throwables) {
      throw throwable;
    }
    for (int i = 100000; i < 110000; i++) {
      assertThat(testSubject.exists(name, i)).isTrue();
    }
  }
}
