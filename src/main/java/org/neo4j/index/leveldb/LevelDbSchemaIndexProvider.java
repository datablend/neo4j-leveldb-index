package org.neo4j.index.leveldb;

import org.iq80.leveldb.*;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.index.*;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;

import java.io.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import static org.iq80.leveldb.impl.Iq80DBFactory.factory;
import static org.neo4j.index.leveldb.LevelDbIndexProviderFactory.PROVIDER_DESCRIPTOR;

/**
 * @author Davy Suvee (http://datablend.be)
 */
public class LevelDbSchemaIndexProvider extends SchemaIndexProvider {

    static int PRIORITY;
    static {
        PRIORITY = 2;
    }

    private final Map<Long, LevelDbIndex> indexes = new CopyOnWriteHashMap<Long, LevelDbIndex>();

    public static byte[] serialize(Object obj) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(out);
            os.writeObject(obj);
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Object deserialize(byte[] data) {
        try {
            if (data != null) {
                ByteArrayInputStream in = new ByteArrayInputStream(data);
                ObjectInputStream is = new ObjectInputStream(in);
                return is.readObject();
            }
            return null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public LevelDbSchemaIndexProvider(Config config) {
        super(PROVIDER_DESCRIPTOR, PRIORITY);
    }

    @Override
    public void shutdown() throws Throwable {
        super.shutdown();
    }

    @Override
    public LevelDbIndex getOnlineAccessor(long indexId) {
        LevelDbIndex index = indexes.get(indexId);
        if (index == null || index.state != InternalIndexState.ONLINE)
            throw new IllegalStateException("Index " + indexId + " not online yet");
        return index;
    }

    @Override
    public InternalIndexState getInitialState(long indexId) {
        LevelDbIndex index = indexes.get(indexId);
        return index != null ? index.state : InternalIndexState.POPULATING;
    }

    @Override
    public LevelDbIndex getPopulator(long indexId) {
        Options options = new Options();
        options.compressionType(CompressionType.NONE);
        options.createIfMissing(true);
        DB db = null;
        try {
            db = factory.open(new File("index" + indexId), options);
        } catch (IOException e) {
            e.printStackTrace();
        }
        LevelDbIndex index = new LevelDbIndex(db);
        indexes.put(indexId, index);
        return index;
    }

    public static class LevelDbIndex extends IndexAccessor.Adapter implements IndexPopulator {

        private final DB db;
        private WriteBatch batch;
        private InternalIndexState state = InternalIndexState.POPULATING;

        public LevelDbIndex(DB db) {
            this.db = db;
            this.batch = db.createWriteBatch();
        }

        @Override
        public void add(long nodeId, Object propertyValue) {
            long[] nodes = (long[])deserialize(db.get(serialize(propertyValue)));
            if (nodes==null || nodes.length==0) {
                batch.put(serialize(propertyValue), serialize(new long[]{nodeId}));
                return;
            }
            int idx=indexOf(nodes,nodeId);
            if (idx!=-1) return;
            nodes = Arrays.copyOfRange(nodes, 0, nodes.length + 1);
            nodes[nodes.length-1]=nodeId;
            batch.put(serialize(propertyValue), serialize(nodes));
        }

        private void removed(long nodeId, Object propertyValue) {
            long[] nodes = (long[])deserialize(db.get(serialize(propertyValue)));
            if (nodes==null || nodes.length ==0) return;
            int idx=indexOf(nodes,nodeId);
            if (idx==-1) return;
            final int existingCount = nodes.length;
            if (existingCount == 1) {
                batch.delete(serialize(propertyValue));
                return;
            }
            System.arraycopy(nodes,idx,nodes,idx-1, existingCount-idx-1);
            nodes = Arrays.copyOfRange(nodes, 0, existingCount - 1);
            batch.put(serialize(propertyValue), serialize(nodes));
        }

        private int indexOf(long[] nodes, long nodeId) {
            for (int i = nodes.length - 1; i != 0; i--) {
                if (nodes[i]==nodeId) return i;
            }
            return -1;
        }

        @Override
        public void update(Iterable<NodePropertyUpdate> updates) {
            for (NodePropertyUpdate update : updates) {
                switch (update.getUpdateMode()) {
                    case ADDED:
                        add(update.getNodeId(), update.getValueAfter());
                        break;
                    case CHANGED:
                        removed(update.getNodeId(), update.getValueBefore());
                        add(update.getNodeId(), update.getValueAfter());
                        break;
                    case REMOVED:
                        removed(update.getNodeId(), update.getValueBefore());
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            }
            db.write(batch);
            batch = db.createWriteBatch();
        }

        @Override
        public void updateAndCommit(Iterable<NodePropertyUpdate> updates) {
            update(updates);
        }

        @Override
        public void recover(Iterable<NodePropertyUpdate> updates) throws IOException {
            update(updates);
        }

        @Override
        public void force() {
            db.write(batch);
            batch = db.createWriteBatch();
        }

        @Override
        public void create() {
            DBIterator it = db.iterator();
            while (it.hasNext()) {
                batch.delete(it.next().getKey());
            }
            db.write(batch);
            batch = db.createWriteBatch();
        }

        @Override
        public void drop() {
            DBIterator it = db.iterator();
            while (it.hasNext()) {
                batch.delete(it.next().getKey());
            }
            db.write(batch);
            batch = db.createWriteBatch();
        }

        @Override
        public void close(boolean populationCompletedSuccessfully) {
            if (populationCompletedSuccessfully)
                state = InternalIndexState.ONLINE;
        }

        @Override
        public void close() {
            db.write(batch);
            batch = db.createWriteBatch();
        }

        /**
         * @return a new {@link org.neo4j.kernel.api.index.IndexReader} responsible for looking up results in the index.
         * The returned reader must honor repeatable reads.
         */
        @Override
        public IndexReader newReader() {
            return new MapDbIndexReader(db);
        }
    }

    private static class MapDbIndexReader implements IndexReader {
        private Snapshot snapshot;
        private DB db;

        MapDbIndexReader(DB db) {
            this.db = db;
            snapshot = db.getSnapshot();
        }

        @Override
        public Iterator<Long> lookup(Object value) {
            ReadOptions ro = new ReadOptions();
            ro.snapshot(snapshot);
            final long[] result = (long[])deserialize(db.get(serialize(value), ro));
            return result == null || result.length==0 ? IteratorUtil.<Long>emptyIterator() : new Iterator<Long>() {
                int idx=0;
                private final int length = result.length;

                @Override
                public boolean hasNext() {
                    return idx < length;
                }

                @Override
                public Long next() {
                    return result[idx++];
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public void close() {
            try {
                snapshot.close();
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            snapshot=null;
        }

    }
}
