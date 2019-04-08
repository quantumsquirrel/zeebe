/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.clustering.base.partitions;

import io.atomix.core.Atomix;
import io.atomix.core.election.Leader;
import io.atomix.core.election.LeaderElection;
import io.atomix.core.election.LeadershipEvent;
import io.atomix.core.election.LeadershipEventListener;
import io.atomix.primitive.PrimitiveState;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.zeebe.broker.Loggers;
import io.zeebe.distributedlog.impl.DistributedLogstreamName;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.slf4j.Logger;

public class PartitionLeaderElection extends Actor
    implements Service<PartitionLeaderElection>,
        LeadershipEventListener<String>,
        Consumer<PrimitiveState> {

  private static final Logger LOG = Loggers.CLUSTERING_LOGGER;

  private final Injector<Atomix> atomixInjector = new Injector<>();
  private Atomix atomix;

  // TODO: Check if we should use memberId instead of string
  private LeaderElection<String> election;

  private static final MultiRaftProtocol PROTOCOL =
      MultiRaftProtocol.builder().withPartitioner(DistributedLogstreamName.getInstance()).build();

  private final int partitionId;
  private String memberId;
  private final List<PartitionRoleChangeListener> leaderElectionListeners;
  private boolean isLeader =
      false; // true if this node was the leader in the last leadership event received.

  public PartitionLeaderElection(int partitionId) {
    this.partitionId = partitionId;
    leaderElectionListeners = new ArrayList<>();
  }

  @Override
  public void start(ServiceStartContext startContext) {

    atomix = atomixInjector.getValue();
    memberId = atomix.getMembershipService().getLocalMember().id().id();

    LOG.info("Creating leader election for partition {} in node {}", partitionId, memberId);

    final CompletableFuture<LeaderElection<String>> leaderElectionCompletableFuture =
        atomix
            .<String>leaderElectionBuilder(DistributedLogstreamName.getPartitionKey(partitionId))
            .withProtocol(PROTOCOL)
            .buildAsync();

    final CompletableActorFuture startFuture = new CompletableActorFuture();
    leaderElectionCompletableFuture.thenAccept(
        e -> {
          election = e;
          election.run(memberId);
          startFuture.complete(null);
          startContext.getScheduler().submitActor(this);
        });

    startContext.async(startFuture, true);
  }

  @Override
  protected void onActorStarted() {
    initListeners();
  }

  private void initListeners() {
    election.addListener(this);
    election.addStateChangeListener(this);
    final Leader<String> currentLeader = election.getLeadership().leader();
    if (memberId.equals(currentLeader.id())) {
      transitionToLeader(currentLeader.term());
    } else {
      transitionToFollower();
    }
  }

  private void transitionToFollower() {
    isLeader = false;
    leaderElectionListeners.forEach(l -> l.onTransitionToFollower(partitionId));
  }

  private void transitionToLeader(long term) {
    isLeader = true;
    leaderElectionListeners.forEach(l -> l.onTransitionToLeader(partitionId, term));
  }

  @Override
  public void stop(ServiceStopContext stopContext) {
    election.async().withdraw(memberId);
  }

  @Override
  public PartitionLeaderElection get() {
    return this;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public Injector<Atomix> getAtomixInjector() {
    return atomixInjector;
  }

  @Override
  public void event(LeadershipEvent<String> leadershipEvent) {
    final Leader<String> newLeader = leadershipEvent.newLeadership().leader();
    if (newLeader == null) {
      return;
    }

    final String memberId = atomix.getMembershipService().getLocalMember().id().id();

    final boolean becomeFollower = isLeader && !memberId.equals(newLeader.id());
    final boolean becomeLeader = !isLeader && memberId.equals(newLeader.id());

    if (becomeFollower) {
      actor.call(this::transitionToFollower);
    } else if (becomeLeader) {
      actor.call(() -> transitionToLeader(newLeader.term()));
    }
  }

  @Override
  public void accept(PrimitiveState primitiveState) {
    switch (primitiveState) {
        // If primitive is not in connected state, this node might miss leadership events. Hence for
        // safety, transition to follower.
      case CLOSED:
      case EXPIRED:
      case SUSPENDED:
        if (isLeader) {
          transitionToFollower();
        }
        break;
      case CONNECTED:
        final Leader<String> currentLeader = election.getLeadership().leader();
        final boolean isCurrentLeader =
            currentLeader != null && memberId.equals(currentLeader.id());
        if (!isLeader && isCurrentLeader) {
          actor.call(() -> transitionToLeader(currentLeader.term()));
        } else if (isLeader && !isCurrentLeader) {
          actor.call(this::transitionToFollower);
        }
    }
  }

  /**
   * add listeners to get notified when the node transition between (stream processor) Leader and
   * Follower roles.
   */
  public void addListener(PartitionRoleChangeListener listener) {
    leaderElectionListeners.add(listener);
  }

  public boolean isLeader() {
    return isLeader;
  }

  public LeaderElection<String> getElection() {
    return election;
  }
}
