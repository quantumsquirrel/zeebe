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

import io.zeebe.logstreams.impl.Loggers;
import io.zeebe.logstreams.spi.SnapshotController;
import io.zeebe.logstreams.state.StateSnapshotMetadata;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.ActorCondition;
import io.zeebe.util.sched.SchedulingHints;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;

public class AsyncSnapshotDirector extends Actor {

  private static final Logger LOG = Loggers.SNAPSHOT_LOGGER;
  private static final String LOG_MSG_WAIT_UNTIL_COMMITTED =
      "Finished taking snapshot, need to wait until last written event position {} is committed, current commit position is {}. After that snapshot can be marked as valid.";
  private static final String ERROR_MSG_ON_RESOLVE_WRITTEN_POS =
      "Unexpected error in resolving last written position.";
  private static final String LOG_MSG_ENFORCE_SNAPSHOT =
      "Enforce snapshot creation for last written position {} with commit position {} and term {}.";
  private static final String ERROR_MSG_ENFORCED_SNAPSHOT =
      "Unexpected exception occured on creating snapshot, was enforced to do so.";
  private static final String ERROR_MSG_ENSURING_MAX_SNAPSHOT_COUNT =
      "Unexpected exception occurred on ensuring maximum snapshot count.";

  static final int INITIAL_POSITION = -1;
  private static final Duration SNAPSHOT_TIME = Duration.ofSeconds(15);
  private static final int MAX_SNAPSHOT_COUNT = 3;

  private final Runnable takeSnapshot = this::takeSnapshot;

  private final Supplier<ActorFuture<Long>> asyncLastWrittenPositionSupplier;

  private final SnapshotController snapshotController;

  private final Consumer<ActorCondition> conditionRegistration;
  private final Consumer<ActorCondition> conditionCheckOut;
  private final IntSupplier termSupplier;
  private final LongSupplier commitPositionSupplier;
  private final String name;

  private ActorCondition commitCondition;
  long lastWrittenEventPosition = INITIAL_POSITION;
  boolean pendingSnapshot;

  AsyncSnapshotDirector(
      String name,
      Supplier<ActorFuture<Long>> asyncLastWrittenPositionSupplier,
      SnapshotController snapshotController,
      Consumer<ActorCondition> conditionRegistration,
      Consumer<ActorCondition> conditionCheckOut,
      IntSupplier termSupplier,
      LongSupplier commitPositionSupplier) {
    this.asyncLastWrittenPositionSupplier = asyncLastWrittenPositionSupplier;
    this.snapshotController = snapshotController;
    this.conditionRegistration = conditionRegistration;
    this.conditionCheckOut = conditionCheckOut;
    this.termSupplier = termSupplier;
    this.commitPositionSupplier = commitPositionSupplier;
    this.name = name + "-snapshot-director";
  }

  @Override
  protected void onActorStarting() {
    actor.setSchedulingHints(SchedulingHints.ioBound());
    actor.runAtFixedRate(SNAPSHOT_TIME, takeSnapshot);

    commitCondition = actor.onCondition(getConditionNameForPosition(), this::onCommitCheck);
    conditionRegistration.accept(commitCondition);
  }

  @Override
  public String getName() {
    return name;
  }

  private String getConditionNameForPosition() {
    return getName() + "-wait-for-endPosition-committed";
  }

  @Override
  protected void onActorCloseRequested() {
    conditionCheckOut.accept(commitCondition);
  }

  ActorFuture<Void> enforceSnapshotCreation(
      final long lastWrittenPosition, final long commitPosition, final int term) {
    final ActorFuture<Void> snapshotCreation = new CompletableActorFuture<>();
    actor.call(
        () -> {
          if (lastWrittenPosition <= commitPosition) {
            LOG.debug(LOG_MSG_ENFORCE_SNAPSHOT, lastWrittenPosition, commitPosition, term);

            final StateSnapshotMetadata metadata =
                new StateSnapshotMetadata(lastWrittenPosition, term, false);
            try {
              snapshotController.takeSnapshot(metadata);
            } catch (Exception ex) {
              LOG.error(ERROR_MSG_ENFORCED_SNAPSHOT, ex);
            }
          }

          snapshotCreation.complete(null);
        });

    return snapshotCreation;
  }

  private void takeSnapshot() {
    if (pendingSnapshot) {
      return;
    }

    snapshotController.takeTempSnapshot();

    final ActorFuture<Long> lastWrittenPosition = asyncLastWrittenPositionSupplier.get();
    actor.runOnCompletion(
        lastWrittenPosition,
        (endPosition, error) -> {
          if (error == null) {
            final long commitPosition = commitPositionSupplier.getAsLong();
            lastWrittenEventPosition = endPosition;
            pendingSnapshot = true;

            if (commitPosition >= lastWrittenEventPosition) {
              onCommitCheck();
            } else {
              LOG.debug(LOG_MSG_WAIT_UNTIL_COMMITTED, endPosition, commitPosition);
            }
          } else {
            LOG.error(ERROR_MSG_ON_RESOLVE_WRITTEN_POS, error);
          }
        });
  }

  private void onCommitCheck() {
    final long currentCommitPosition = commitPositionSupplier.getAsLong();
    final int currentTerm = termSupplier.getAsInt();

    if (pendingSnapshot && currentCommitPosition >= lastWrittenEventPosition) {
      final StateSnapshotMetadata stateSnapshotMetadata =
          new StateSnapshotMetadata(lastWrittenEventPosition, currentTerm, false);
      snapshotController.moveSnapshot(stateSnapshotMetadata);
      pendingSnapshot = false;

      try {
        snapshotController.ensureMaxSnapshotCount(MAX_SNAPSHOT_COUNT);
      } catch (Exception ex) {
        LOG.error(ERROR_MSG_ENSURING_MAX_SNAPSHOT_COUNT, ex);
      }
    }
  }

  public void close() {
    actor.close();
  }
}
