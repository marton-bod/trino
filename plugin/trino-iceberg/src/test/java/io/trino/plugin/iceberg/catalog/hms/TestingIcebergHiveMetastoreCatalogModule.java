/*
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
package io.trino.plugin.iceberg.catalog.hms;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.hive.metastore.DecoratedHiveMetastoreModule;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.RawHiveMetastoreFactory;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreFactory;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TestingIcebergHiveMetastoreCatalogModule
        extends AbstractConfigurationAwareModule
{
    private final HiveMetastore metastore;
    private final ThriftMetastore thriftMetastore;
    private final ThriftMetastoreFactory thriftMetastoreFactory;
    private final HiveMetastoreFactory hiveMetastoreFactory;

    public TestingIcebergHiveMetastoreCatalogModule(ThriftMetastore thriftMetastore)
    {
        this.thriftMetastore = requireNonNull(thriftMetastore, "thriftMetastore is null");
        this.metastore = new BridgingHiveMetastore(thriftMetastore);
        this.thriftMetastoreFactory = buildThriftMetastoreFactory(this.thriftMetastore);
        this.hiveMetastoreFactory = buildHiveMetastoreFactory(this.metastore);
    }

    public HiveMetastore getHiveMetastore()
    {
        return metastore;
    }

    @Override
    protected void setup(Binder binder)
    {
        install(new DecoratedHiveMetastoreModule());
        binder.bind(ThriftMetastoreFactory.class).toInstance(this.thriftMetastoreFactory);
        binder.bind(HiveMetastoreFactory.class).annotatedWith(RawHiveMetastoreFactory.class).toInstance(this.hiveMetastoreFactory);
        binder.bind(IcebergTableOperationsProvider.class).to(HiveMetastoreTableOperationsProvider.class).in(Scopes.SINGLETON);
        binder.bind(TrinoCatalogFactory.class).to(TrinoHiveCatalogFactory.class).in(Scopes.SINGLETON);
    }

    private ThriftMetastoreFactory buildThriftMetastoreFactory(ThriftMetastore thriftMetastore)
    {
        return new ThriftMetastoreFactory()
        {
            @Override
            public boolean isImpersonationEnabled()
            {
                return false;
            }

            @Override
            public ThriftMetastore createMetastore(Optional<ConnectorIdentity> identity)
            {
                return thriftMetastore;
            }
        };
    }

    private HiveMetastoreFactory buildHiveMetastoreFactory(HiveMetastore metastore)
    {
        return new HiveMetastoreFactory()
        {
            @Override
            public boolean isImpersonationEnabled()
            {
                return false;
            }

            @Override
            public HiveMetastore createMetastore(Optional<ConnectorIdentity> identity)
            {
                return metastore;
            }
        };
    }
}
