package org.jobrunr.storage.nosql.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import org.jobrunr.storage.nosql.NoSqlStorageProvider;
import org.jobrunr.storage.nosql.common.NoSqlDatabaseCreator;
import org.jobrunr.storage.nosql.common.migrations.NoSqlMigration;
import org.jobrunr.storage.nosql.redis.migrations.LettuceRedisMigration;

public class LettuceRedisDBCreator extends NoSqlDatabaseCreator<LettuceRedisMigration> {

    private final StatefulRedisConnection<String, String> connection;
    private final String keyPrefix;

    public LettuceRedisDBCreator(NoSqlStorageProvider noSqlStorageProvider, StatefulRedisConnection<String, String> connection, String keyPrefix) {
        super(noSqlStorageProvider);
        this.connection = connection;
        this.keyPrefix = keyPrefix;
    }

    @Override
    protected boolean isValidMigration(NoSqlMigration noSqlMigration) {
        return noSqlMigration.getClassName().contains("Lettuce");
    }

    @Override
    protected boolean isNewMigration(NoSqlMigration noSqlMigration) {
        // why: as Jedis does not have a schema, each migration checks if it needs to do something
        return true;
    }

    @Override
    protected void runMigration(LettuceRedisMigration noSqlMigration) throws Exception {
        noSqlMigration.runMigration(connection, keyPrefix);
    }

    @Override
    protected boolean markMigrationAsDone(NoSqlMigration noSqlMigration) {
        return true;
    }

}
