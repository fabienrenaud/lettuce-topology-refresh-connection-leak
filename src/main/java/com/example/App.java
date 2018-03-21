package com.example;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.codec.Utf8StringCodec;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.resource.Delay;

import java.time.Duration;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class App {

    private static final String REDIS_HOST = "localhost";
    private static final int REDIS_PORT = 7000;

    public static void main(String[] args) {
        RedisURI redisUri = RedisURI.Builder
                .redis(REDIS_HOST, REDIS_PORT)
                .build();

        ClientResources clientResources = DefaultClientResources.builder()
//                .eventExecutorGroup() // TODO?
                .reconnectDelay(Delay.constant(Duration.ofSeconds(5)))
                .build();
        RedisClusterClient client = RedisClusterClient.create(clientResources, redisUri);

        ClusterTopologyRefreshOptions topologyRefreshOptions = ClusterTopologyRefreshOptions.builder()
                .enablePeriodicRefresh()
                .refreshPeriod(Duration.ofSeconds(1))
                .enableAllAdaptiveRefreshTriggers()
                .refreshTriggersReconnectAttempts(5)
                .dynamicRefreshSources(true)
                .build();

        ClusterClientOptions.Builder ccoBuilder = ClusterClientOptions.builder()
                .topologyRefreshOptions(topologyRefreshOptions)
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .cancelCommandsOnReconnectFailure(true)
                .socketOptions(SocketOptions.builder()
                        .keepAlive(true)
                        .build());

        client.setOptions(ccoBuilder.build());
        StatefulRedisClusterConnection<String, String> c = client.connect(new Utf8StringCodec());
        RedisAdvancedClusterAsyncCommands<String, String> commands = c.async();

        Runtime.getRuntime().addShutdownHook(new Thread(client::shutdown));

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            System.out.printf("%s: still alive\n", new Date());
            commands.clientList().thenAccept(System.out::println);
        }, 0, 5, TimeUnit.SECONDS);
    }
}
