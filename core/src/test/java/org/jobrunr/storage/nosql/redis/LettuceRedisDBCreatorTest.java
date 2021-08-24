package org.jobrunr.storage.nosql.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import org.jobrunr.storage.nosql.common.migrations.NoSqlMigrationByClass;
import org.jobrunr.storage.nosql.redis.migrations.M001_JedisRemoveJobStatsAndUseMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

@Testcontainers
@ExtendWith(MockitoExtension.class)
class LettuceRedisDBCreatorTest {

    @Container
    private static final GenericContainer redisContainer = new GenericContainer("redis").withExposedPorts(6379);

    @Mock
    private LettuceRedisStorageProvider lettuceRedisStorageProviderMock;
    private StatefulRedisConnection<String, String> connection;
    private LettuceRedisDBCreator lettuceRedisDBCreator;

    @BeforeEach
    public void setupDBCreator() {
        connection = getRedisClient().connect();
        lettuceRedisDBCreator = new LettuceRedisDBCreator(lettuceRedisStorageProviderMock, connection, "");
    }

    @AfterEach
    public void teardownPool() {
        connection.close();
    }

    @Test
    void testMigrationsHappyPath() {
        assertThat(lettuceRedisDBCreator.isNewMigration(new NoSqlMigrationByClass(M001_JedisRemoveJobStatsAndUseMetadata.class))).isTrue();

        assertThatCode(lettuceRedisDBCreator::runMigrations).doesNotThrowAnyException();
        assertThatCode(lettuceRedisDBCreator::runMigrations).doesNotThrowAnyException();

        assertThat(lettuceRedisDBCreator.isNewMigration(new NoSqlMigrationByClass(M001_JedisRemoveJobStatsAndUseMetadata.class))).isTrue();

    }

    private RedisClient getRedisClient() {
        return RedisClient.create(RedisURI.create(redisContainer.getContainerIpAddress(), redisContainer.getMappedPort(6379)));
    }
}