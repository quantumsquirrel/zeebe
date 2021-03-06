/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.util.retry;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.testing.ControlledActorSchedulerRule;
import java.lang.reflect.Constructor;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class RetryStrategyTest {

  @Rule public ControlledActorSchedulerRule schedulerRule = new ControlledActorSchedulerRule();

  @Parameters(name = "{index}: {0}")
  public static Object[][] reprocessingTriggers() {
    return new Object[][] {
      new Object[] {RecoverableRetryStrategy.class}, new Object[] {AbortableRetryStrategy.class}
    };
  }

  @Parameter public Class<RetryStrategy> retryStrategyClass;

  private RetryStrategy retryStrategy;
  private ActorControl actorControl;
  private ActorFuture<Boolean> resultFuture;

  @Before
  public void setUp() {
    final ControllableActor actor = new ControllableActor();
    this.actorControl = actor.getActor();

    try {
      final Constructor<RetryStrategy> constructor =
          retryStrategyClass.getConstructor(ActorControl.class);
      retryStrategy = constructor.newInstance(actorControl);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    schedulerRule.submitActor(actor);
  }

  @Test
  public void shouldRunUntilDone() throws Exception {
    // given
    final AtomicInteger count = new AtomicInteger(0);

    // when
    actorControl.run(
        () -> {
          resultFuture = retryStrategy.runWithRetry(() -> count.incrementAndGet() == 10);
        });

    schedulerRule.workUntilDone();

    // then
    assertThat(count.get()).isEqualTo(10);
    assertThat(resultFuture.isDone()).isTrue();
    assertThat(resultFuture.get()).isTrue();
  }

  @Test
  public void shouldStopWhenAbortConditionReturnsTrue() throws Exception {
    // given
    final AtomicInteger count = new AtomicInteger(0);

    // when
    actorControl.run(
        () -> {
          resultFuture =
              retryStrategy.runWithRetry(() -> false, () -> count.incrementAndGet() == 10);
        });

    schedulerRule.workUntilDone();

    // then
    assertThat(count.get()).isEqualTo(10);
    assertThat(resultFuture.isDone()).isTrue();
    assertThat(resultFuture.get()).isFalse();
  }

  @Test
  public void shouldAbortOnOtherException() {
    // given
    // when
    actorControl.run(
        () ->
            resultFuture =
                retryStrategy.runWithRetry(
                    () -> {
                      throw new RuntimeException("expected");
                    }));

    schedulerRule.workUntilDone();

    // then
    assertThat(resultFuture.isDone()).isTrue();
    assertThat(resultFuture.isCompletedExceptionally()).isTrue();
    assertThat(resultFuture.getException()).isExactlyInstanceOf(RuntimeException.class);
  }

  private static final class ControllableActor extends Actor {
    public ActorControl getActor() {
      return actor;
    }
  }
}
