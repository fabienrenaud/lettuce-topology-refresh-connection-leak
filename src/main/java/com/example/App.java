package com.example;


import com.lambdaworks.redis.ClientOptions;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.SocketOptions;
import com.lambdaworks.redis.cluster.ClusterClientOptions;
import com.lambdaworks.redis.cluster.ClusterTopologyRefreshOptions;
import com.lambdaworks.redis.cluster.RedisClusterClient;
import com.lambdaworks.redis.cluster.api.StatefulRedisClusterConnection;
import com.lambdaworks.redis.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import com.lambdaworks.redis.codec.Utf8StringCodec;
import com.lambdaworks.redis.resource.ClientResources;
import com.lambdaworks.redis.resource.DefaultClientResources;
import com.lambdaworks.redis.resource.Delay;

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
                .reconnectDelay(Delay.constant(5, TimeUnit.SECONDS))
                .build();
        RedisClusterClient client = RedisClusterClient.create(clientResources, redisUri);

        ClusterTopologyRefreshOptions topologyRefreshOptions = ClusterTopologyRefreshOptions.builder()
                .enablePeriodicRefresh()
                .refreshPeriod(1, TimeUnit.SECONDS)
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
