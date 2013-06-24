package org.neo4j.index.leveldb;

import org.junit.Before;

import java.io.IOException;

/**
 * @author mh
 * @since 06.05.13
 */
public class LuceneIndexTest extends BasicIndexTest {
    @Override
    @Before
    public void setUp() throws IOException {
        LevelDbSchemaIndexProvider.PRIORITY = 0;
        super.setUp();
    }
}
