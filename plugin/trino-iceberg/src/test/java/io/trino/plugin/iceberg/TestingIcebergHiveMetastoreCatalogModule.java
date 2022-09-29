package io.trino.plugin.iceberg;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.hive.HideDeltaLakeTables;
import io.trino.plugin.hive.metastore.DecoratedHiveMetastoreModule;
import io.trino.plugin.hive.metastore.thrift.*;
import io.trino.plugin.iceberg.catalog.IcebergCatalogModule;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.catalog.hms.HiveMetastoreTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalogFactory;
import io.trino.spi.security.ConnectorIdentity;

import java.io.File;
import java.util.Optional;

/**
 * I wanted to include this into IcebergCatalogModule.setup()
 * under the new value CatalogType.TESTING_IN_MEMORY_METASTORE
 */
public class TestingIcebergHiveMetastoreCatalogModule
        extends AbstractConfigurationAwareModule
    {
        public static final boolean HIDE_DELTA_LAKE_TABLES_IN_ICEBERG = false;

        @Override
        protected void setup(Binder binder)
        {
            install(new InMemoryThriftMetastoreModule());
            binder.bind(IcebergTableOperationsProvider.class).to(HiveMetastoreTableOperationsProvider.class).in(Scopes.SINGLETON);
            binder.bind(TrinoCatalogFactory.class).to(TrinoHiveCatalogFactory.class).in(Scopes.SINGLETON);
            binder.bind(IcebergCatalogModule.MetastoreValidator.class).asEagerSingleton();
            binder.bind(Key.get(boolean.class, TranslateHiveViews.class)).toInstance(false);
            binder.bind(Key.get(boolean.class, HideDeltaLakeTables.class)).toInstance(HIDE_DELTA_LAKE_TABLES_IN_ICEBERG);
            install(new DecoratedHiveMetastoreModule());
        }

        private static class InMemoryThriftMetastoreModule extends ThriftMetastoreModule {
            @Override
            protected void setup(Binder binder) {
                super.setup(binder);
                binder.bind(ThriftMetastoreFactory.class).to(InMemoryThriftMetastoreFactory.class).in(Scopes.SINGLETON);
            }
        }

        private static class InMemoryThriftMetastoreFactory implements ThriftMetastoreFactory {
            @Override
            public boolean isImpersonationEnabled() {
                return false;
            }

            @Override
            public ThriftMetastore createMetastore(Optional<ConnectorIdentity> identity) {
                return new InMemoryThriftMetastore(new File("/tmp/metastore/"), new ThriftMetastoreConfig());
            }
        }
}
