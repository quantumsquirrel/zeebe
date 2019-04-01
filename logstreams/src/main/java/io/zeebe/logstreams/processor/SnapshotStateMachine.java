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
import io.zeebe.util.collection.Tuple;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.ActorCondition;
import io.zeebe.util.sched.SchedulingHints;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.time.Duration;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;

public class SnapshotStateMachine extends Actor {

  private static final Logger LOG = Loggers.SNAPSHOT_LOGGER;

  private static final Duration SNAPSHOT_TIME = Duration.ofSeconds(15);
  private static final int MAX_SNAPSHOT_COUNT = 3;

  private final Runnable startSnapshot = this::prepareToTakeSnapshot;

  final NavigableMap<Long, Tuple<Long, Long>> pendingSnapshots = new TreeMap<>();

  private final Supplier<ActorFuture<Long>> asyncLastProcessedPositionSupplier;
  private final Supplier<ActorFuture<Long>> asyncLastWrittenPositionSupplier;

  private final SnapshotController snapshotController;

  private final Consumer<ActorCondition> conditionRegistration;
  private final Consumer<ActorCondition> conditionCheckOut;
  private final IntSupplier termSupplier;
  private final LongSupplier commitPositionSupplier;
  private ActorCondition commitCondition;

  public SnapshotStateMachine(
      Supplier<ActorFuture<Long>> asyncLastProcessedPositionSupplier,
      Supplier<ActorFuture<Long>> asyncLastWrittenPositionSupplier,
      SnapshotController snapshotController,
      Consumer<ActorCondition> conditionRegistration,
      Consumer<ActorCondition> conditionCheckOut,
      IntSupplier termSupplier,
      LongSupplier commitPositionSupplier) {
    this.asyncLastProcessedPositionSupplier = asyncLastProcessedPositionSupplier;
    this.asyncLastWrittenPositionSupplier = asyncLastWrittenPositionSupplier;
    this.snapshotController = snapshotController;
    this.conditionRegistration = conditionRegistration;
    this.conditionCheckOut = conditionCheckOut;
    this.termSupplier = termSupplier;
    this.commitPositionSupplier = commitPositionSupplier;
  }

  @Override
  protected void onActorStarting() {
    actor.setSchedulingHints(SchedulingHints.ioBound());
    actor.runAtFixedRate(SNAPSHOT_TIME, startSnapshot);

    commitCondition = actor.onCondition(getConditionNameForPosition(), this::onCommitCheck);
    conditionRegistration.accept(commitCondition);
  }

  private String getConditionNameForPosition() {
    return getName() + "-wait-for-endPosition-committed";
  }

  @Override
  protected void onActorCloseRequested() {
    conditionCheckOut.accept(commitCondition);
  }

  public ActorFuture<Void> enforceSnapshotCreation(
      final long snapshotPosition,
      final long lastWrittenPosition,
      final long commitPosition,
      final int term) {
    final ActorFuture<Void> snapshotCreation = new CompletableActorFuture<>();
    actor.call(
        () -> {
          if (lastWrittenPosition <= commitPosition) {
            LOG.info(
                "Enforce snapshot creation for last processed position {},"
                    + " last written position {} with commit position {} and term {}.",
                snapshotPosition,
                lastWrittenPosition,
                commitPosition,
                term);
            final StateSnapshotMetadata metadata =
                new StateSnapshotMetadata(snapshotPosition, lastWrittenPosition, term, false);
            try {
              snapshotController.takeSnapshot(metadata);
            } catch (Exception ex) {
              LOG.error(
                  "Unexpected exception occured on creating snapshot, was enforced to do so.", ex);
            }
          }

          snapshotCreation.complete(null);
        });

    return snapshotCreation;
  }

  private void prepareToTakeSnapshot() {
    final ActorFuture<Long> currentPosition = asyncLastProcessedPositionSupplier.get();
    actor.runOnCompletion(
        currentPosition,
        (pos, error) -> {
          if (error == null) {
            LOG.info("Time based snapshotting will take snapshot at position {}", pos);
            takeSnapshot(pos);
          } else {
            LOG.error("Unexpected error in resolving current processing position.", error);
          }
        });
  }

  private void takeSnapshot(final long startPosition) {
    snapshotController.takeSnapshotForPosition(startPosition);

    final ActorFuture<Long> lastProcessedPosition = asyncLastProcessedPositionSupplier.get();
    actor.runOnCompletion(
        lastProcessedPosition,
        (position, error) -> {
          if (error == null) {
            finishSnapshot(startPosition, position);
          } else {
            LOG.error("Unexpected error in resolving last processed position.", error);
          }
        });
  }

  private void finishSnapshot(final long startPosition, final long snapshotEndPosition) {
    final ActorFuture<Long> lastWrittenPosition = asyncLastWrittenPositionSupplier.get();
    actor.runOnCompletion(
        lastWrittenPosition,
        (endPosition, error) -> {
          if (error == null) {
            LOG.info(
                "Finished time based snapshotting for position {}, need to wait until end position {} is committed."
                    + " After that snapshot can be marked as valid.",
                startPosition,
                endPosition);
            pendingSnapshots.put(endPosition, new Tuple<>(startPosition, snapshotEndPosition));
          } else {
            LOG.error("Unexpected error in resolving last written position.", error);
          }
        });
  }

  private void onCommitCheck() {
    final long currentCommitPosition = commitPositionSupplier.getAsLong();
    final int currentTerm = termSupplier.getAsInt();

    final NavigableMap<Long, Tuple<Long, Long>> validSnapshots =
        pendingSnapshots.headMap(currentCommitPosition, true);

    while (!validSnapshots.isEmpty()) {
      final Entry<Long, Tuple<Long, Long>> validSnapshot = validSnapshots.pollFirstEntry();
      final Tuple<Long, Long> tuple = validSnapshot.getValue();
      final Long startPosition = tuple.getLeft();
      final Long snapshotEndPosition = tuple.getRight();
      final Long lastWrittenPosition = validSnapshot.getKey();

      final StateSnapshotMetadata stateSnapshotMetadata =
          new StateSnapshotMetadata(snapshotEndPosition, lastWrittenPosition, currentTerm, false);
      snapshotController.moveSnapshot(startPosition, stateSnapshotMetadata);
    }

    try {
      snapshotController.ensureMaxSnapshotCount(MAX_SNAPSHOT_COUNT);
    } catch (Exception ex) {
      LOG.error("Unexpected exception occurred on ensuring maximum snapshot count.", ex);
    }
  }
}
