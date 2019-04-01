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
package io.zeebe.logstreams.processor;

import static io.zeebe.test.util.TestUtil.waitUntil;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.when;

import io.zeebe.db.impl.DefaultColumnFamily;
import io.zeebe.db.impl.rocksdb.ZeebeRocksDbFactory;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.state.StateSnapshotController;
import io.zeebe.logstreams.state.StateSnapshotMetadata;
import io.zeebe.logstreams.state.StateStorage;
import io.zeebe.logstreams.util.LogStreamRule;
import io.zeebe.test.util.AutoCloseableRule;
import io.zeebe.util.sched.ActorScheduler;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.mockito.InOrder;
import org.mockito.Mockito;

public class AsyncSnapshotingTest {

  private final TemporaryFolder tempFolderRule = new TemporaryFolder();
  private final AutoCloseableRule autoCloseableRule = new AutoCloseableRule();
  private final LogStreamRule logStreamRule = new LogStreamRule(tempFolderRule);

  @Rule
  public final RuleChain chain =
      RuleChain.outerRule(autoCloseableRule).around(tempFolderRule).around(logStreamRule);

  private StateSnapshotController snapshotController;
  private LogStream logStream;
  private SnapshotStateMachine snapshotStateMachine;

  @Before
  public void setup() throws IOException {
    final File snapshotsDirectory = tempFolderRule.newFolder("snapshots");
    final File runtimeDirectory = tempFolderRule.newFolder("runtime");
    final StateStorage storage = new StateStorage(runtimeDirectory, snapshotsDirectory);

    snapshotController =
        new StateSnapshotController(
            ZeebeRocksDbFactory.newFactory(DefaultColumnFamily.class), storage);
    snapshotController.openDb();
    autoCloseableRule.manage(snapshotController);
    snapshotController = spy(snapshotController);

    logStreamRule.setCommitPosition(25L);
    logStream = spy(logStreamRule.getLogStream());
    final ActorScheduler actorScheduler = logStreamRule.getActorScheduler();

    final Supplier<ActorFuture<Long>> positionSupplier = mock(Supplier.class);
    when(positionSupplier.get())
        // start snapshot A
        .thenReturn(CompletableActorFuture.completed(25L))
        // end snapshot A
        .thenReturn(CompletableActorFuture.completed(32L))

        // start snapshot B
        .thenReturn(CompletableActorFuture.completed(35L))
        // end snapshot B
        .thenReturn(CompletableActorFuture.completed(41L));

    final Supplier<ActorFuture<Long>> writtenSupplier = mock(Supplier.class);
    when(writtenSupplier.get())
        .thenReturn(CompletableActorFuture.completed(99L), CompletableActorFuture.completed(100L));

    snapshotStateMachine =
        new SnapshotStateMachine(
            positionSupplier,
            writtenSupplier,
            snapshotController,
            actorCondition -> logStream.registerOnCommitPositionUpdatedCondition(actorCondition),
            actorCondition -> logStream.removeOnCommitPositionUpdatedCondition(actorCondition),
            () -> logStream.getTerm(),
            () -> logStream.getCommitPosition());
    actorScheduler.submitActor(snapshotStateMachine).join();
  }

  @Test
  public void shouldStartToTakeSnapshot() {
    // given

    // when
    logStreamRule.getClock().addTime(Duration.ofMinutes(1));

    // then
    final InOrder inOrder = Mockito.inOrder(snapshotController, logStream);
    inOrder
        .verify(logStream, timeout(500L).times(1))
        .registerOnCommitPositionUpdatedCondition(any());
    inOrder.verify(snapshotController, timeout(500L).times(1)).takeSnapshotForPosition(25L);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldWaitUntilCommitPositionWasUpdated() throws Exception {
    // given
    logStreamRule.getClock().addTime(Duration.ofMinutes(1));

    // when
    waitUntil(() -> snapshotStateMachine.pendingSnapshots.size() > 0);
    logStreamRule.setCommitPosition(100L);

    // then
    final InOrder inOrder = Mockito.inOrder(snapshotController, logStream);
    inOrder
        .verify(logStream, timeout(500L).times(1))
        .registerOnCommitPositionUpdatedCondition(any());
    inOrder.verify(snapshotController, timeout(500L).times(1)).takeSnapshotForPosition(25L);

    inOrder.verify(logStream, timeout(500L).times(1)).getCommitPosition();
    inOrder.verify(logStream, timeout(500L).times(1)).getTerm();

    final StateSnapshotMetadata snapshotMetadata = new StateSnapshotMetadata(32L, 99L, 0, false);
    inOrder
        .verify(snapshotController, timeout(500L).times(1))
        .moveSnapshot(eq(25L), eq(snapshotMetadata));

    inOrder.verify(snapshotController, timeout(500L).times(1)).ensureMaxSnapshotCount(3);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldAddMoreThenOneSnapshotPending() {
    // given

    // when
    logStreamRule.getClock().addTime(Duration.ofMinutes(1));
    waitUntil(() -> snapshotStateMachine.pendingSnapshots.size() > 0);
    logStreamRule.getClock().addTime(Duration.ofMinutes(1));
    waitUntil(() -> snapshotStateMachine.pendingSnapshots.size() > 1);

    // then
    final InOrder inOrder = Mockito.inOrder(snapshotController, logStream);
    inOrder
        .verify(logStream, timeout(500L).times(1))
        .registerOnCommitPositionUpdatedCondition(any());

    inOrder.verify(snapshotController, timeout(500L).times(1)).takeSnapshotForPosition(25L);
    inOrder.verify(snapshotController, timeout(500L).times(1)).takeSnapshotForPosition(35L);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldValidateMoreThenOneSnapshot() throws Exception {
    // given
    logStreamRule.getClock().addTime(Duration.ofMinutes(1));
    waitUntil(() -> snapshotStateMachine.pendingSnapshots.size() > 0);
    logStreamRule.getClock().addTime(Duration.ofMinutes(1));
    waitUntil(() -> snapshotStateMachine.pendingSnapshots.size() > 1);

    // when
    logStreamRule.setCommitPosition(100L);

    // then
    final InOrder inOrder = Mockito.inOrder(snapshotController, logStream);

    inOrder.verify(logStream, timeout(500L).times(1)).getCommitPosition();
    inOrder.verify(logStream, timeout(500L).times(1)).getTerm();

    StateSnapshotMetadata snapshotMetadata = new StateSnapshotMetadata(32L, 99L, 0, false);
    inOrder
        .verify(snapshotController, timeout(500L).times(1))
        .moveSnapshot(eq(25L), eq(snapshotMetadata));

    snapshotMetadata = new StateSnapshotMetadata(41L, 100L, 0, false);
    inOrder
        .verify(snapshotController, timeout(500L).times(1))
        .moveSnapshot(eq(35L), eq(snapshotMetadata));

    inOrder.verify(snapshotController, timeout(500L).times(1)).ensureMaxSnapshotCount(3);

    inOrder.verifyNoMoreInteractions();
  }
}
