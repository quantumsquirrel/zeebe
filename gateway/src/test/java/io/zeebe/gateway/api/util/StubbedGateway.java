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
package io.zeebe.gateway.api.util;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.zeebe.gateway.Gateway;
import io.zeebe.gateway.cmd.BrokerErrorException;
import io.zeebe.gateway.cmd.BrokerRejectionException;
import io.zeebe.gateway.cmd.BrokerResponseException;
import io.zeebe.gateway.cmd.IllegalBrokerResponseException;
import io.zeebe.gateway.impl.broker.BrokerClient;
import io.zeebe.gateway.impl.broker.BrokerResponseConsumer;
import io.zeebe.gateway.impl.broker.cluster.BrokerClusterState;
import io.zeebe.gateway.impl.broker.cluster.BrokerClusterStateImpl;
import io.zeebe.gateway.impl.broker.cluster.BrokerTopologyManager;
import io.zeebe.gateway.impl.broker.request.BrokerRequest;
import io.zeebe.gateway.impl.broker.response.BrokerResponse;
import io.zeebe.gateway.impl.configuration.GatewayCfg;
import io.zeebe.gateway.protocol.GatewayGrpc;
import io.zeebe.gateway.protocol.GatewayGrpc.GatewayBlockingStub;
import io.zeebe.protocol.PartitionState;
import io.zeebe.protocol.impl.data.cluster.TopologyResponseDto;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@SuppressWarnings({"rawtypes", "unchecked"})
public class StubbedGateway extends Gateway {

  private static final String SERVER_NAME = "server";

  private Map<Class<?>, RequestHandler> requestHandlers = new HashMap<>();
  private List<BrokerRequest> brokerRequests = new ArrayList<>();

  public StubbedGateway() {
    super(new GatewayCfg(), cfg -> InProcessServerBuilder.forName(SERVER_NAME));
  }

  public <RequestT extends BrokerRequest<?>, ResponseT extends BrokerResponse<?>>
      void registerHandler(
          Class<?> requestType, RequestHandler<RequestT, ResponseT> requestHandler) {
    requestHandlers.put(requestType, requestHandler);
  }

  public GatewayBlockingStub buildClient() {
    final ManagedChannel channel =
        InProcessChannelBuilder.forName(SERVER_NAME).directExecutor().build();
    return GatewayGrpc.newBlockingStub(channel);
  }

  @Override
  protected BrokerClient buildBrokerClient() {
    return new StubbedBrokerClient();
  }

  public <T extends BrokerRequest<?>> T getSingleBrokerRequest() {
    assertThat(brokerRequests).hasSize(1);
    return (T) brokerRequests.get(0);
  }

  private class StubbedBrokerClient implements BrokerClient {

    BrokerTopologyManager topologyManager = new StubbedTopologyManager();

    @Override
    public void close() {}

    @Override
    public <T> ActorFuture<BrokerResponse<T>> sendRequest(BrokerRequest<T> request) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public <T> void sendRequest(
        BrokerRequest<T> request,
        BrokerResponseConsumer<T> responseConsumer,
        Consumer<Throwable> throwableConsumer) {
      brokerRequests.add(request);
      try {
        final RequestHandler requestHandler = requestHandlers.get(request.getClass());
        final BrokerResponse<T> response = requestHandler.handle(request);
        if (response.isResponse()) {
          responseConsumer.accept(response.getKey(), response.getResponse());
        } else if (response.isRejection()) {
          throwableConsumer.accept(new BrokerRejectionException(response.getRejection()));
        } else if (response.isError()) {
          throwableConsumer.accept(new BrokerErrorException(response.getError()));
        } else {
          throwableConsumer.accept(
              new IllegalBrokerResponseException("Unknown response received: " + response));
        }
      } catch (Exception e) {
        throwableConsumer.accept(new BrokerResponseException(e));
      }
    }

    @Override
    public BrokerTopologyManager getTopologyManager() {
      return topologyManager;
    }
  }

  private class StubbedTopologyManager implements BrokerTopologyManager {

    private BrokerClusterState clusterState;

    StubbedTopologyManager() {
      final TopologyResponseDto defaultTopology = new TopologyResponseDto();
      defaultTopology
          .setPartitionsCount(1)
          .setClusterSize(1)
          .setReplicationFactor(1)
          .brokers()
          .add()
          .setHost("localhost")
          .setPort(26501)
          .setNodeId(0)
          .partitionStates()
          .add()
          .setPartitionId(0)
          .setReplicationFactor(1)
          .setState(PartitionState.LEADER);

      provideTopology(defaultTopology);
    }

    @Override
    public BrokerClusterState getTopology() {
      return clusterState;
    }

    @Override
    public ActorFuture<BrokerClusterState> requestTopology() {
      return CompletableActorFuture.completed(clusterState);
    }

    @Override
    public void withTopology(Consumer<BrokerClusterState> topologyConsumer) {
      topologyConsumer.accept(clusterState);
    }

    @Override
    public void provideTopology(TopologyResponseDto topology) {
      clusterState = new BrokerClusterStateImpl(topology, (id, addr) -> {});
    }
  }

  @FunctionalInterface
  interface RequestHandler<RequestT extends BrokerRequest<?>, ResponseT extends BrokerResponse<?>> {
    ResponseT handle(RequestT request) throws Exception;
  }

  public interface RequestStub<
          RequestT extends BrokerRequest<?>, ResponseT extends BrokerResponse<?>>
      extends RequestHandler<RequestT, ResponseT> {
    void registerWith(StubbedGateway gateway);
  }
}
