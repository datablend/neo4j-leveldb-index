package org.neo4j.index.leveldb;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * @author Davy Suvee (http://datablend.be)
 */
@Service.Implementation(KernelExtensionFactory.class)
public class LevelDbIndexProviderFactory extends KernelExtensionFactory<LevelDbIndexProviderFactory.Dependencies> {
    public static final String KEY = "leveldb-index";

    public static final SchemaIndexProvider.Descriptor PROVIDER_DESCRIPTOR =
            new SchemaIndexProvider.Descriptor(KEY, "1.0");

    private final LevelDbSchemaIndexProvider singleProvider;

    public interface Dependencies {
        Config getConfig();
    }

    public LevelDbIndexProviderFactory() {
        this(null);
    }

    public LevelDbIndexProviderFactory(LevelDbSchemaIndexProvider singleProvider) {
        super(KEY);
        this.singleProvider = singleProvider;
    }

    @Override
    public Lifecycle newKernelExtension(Dependencies dependencies) throws Throwable {
        return hasSingleProvider() ? singleProvider : new LevelDbSchemaIndexProvider(dependencies.getConfig());
    }

    private boolean hasSingleProvider() {
        return singleProvider != null;
    }

}
