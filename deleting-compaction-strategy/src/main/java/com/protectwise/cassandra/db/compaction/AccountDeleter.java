package com.protectwise.cassandra.db.compaction;

/**
 * Created by jhaddad on 12/29/16.
 */

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.exceptions.ConfigurationException;

import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/*
used for bulk deletion, typically for migrating accounts between clusters

CREATE TABLE deleted_account (
accountid uuid primary key
);


 */
public class AccountDeleter extends AbstractClusterDeletingConvictor {
    /**
     * @param cfs
     * @param options
     */
    private static final Logger logger = LoggerFactory.getLogger(AccountDeleter.class);

    private static String DELETED_QUERY_KEY = "deleted_query";
    private static String DELETED_QUERY;

    private static ByteBuffer FIELD_TO_MATCH;

    private HashSet<ByteBuffer> deleted_accounts;

    public AccountDeleter(ColumnFamilyStore cfs, Map<String, String> options) {
        super(cfs, options);

        DELETED_QUERY = options.get(DELETED_QUERY_KEY);
        FIELD_TO_MATCH = ByteBufferUtil.bytes(options.get("field_to_match"));

        logger.info("Using account deleter with {}", DELETED_QUERY);

        UntypedResultSet rows = QueryProcessor.executeInternal(DELETED_QUERY);
        logger.info("AccountDeleter created, {} accounts purgable.", rows.size());

        deleted_accounts = new HashSet<>();

        for(UntypedResultSet.Row row: rows) {
            // TODO: field is hard coded for POC, please fix
            logger.info("Row {} marked for deletion", row.getBytes("videoid"));
            deleted_accounts.add(row.getBytes("videoid"));
        }
    }

    @Override
    public boolean shouldKeepPartition(OnDiskAtomIterator key) {
        logger.info("examining {}", key);
        for (Map.Entry<ByteBuffer, ByteBuffer> e : this.getNamedPkColumns(key).entrySet())
        {
//            logger.info("Checking {}", e.getValue());
            // TODO make sure we're looking at the right key
            if (e.getKey() == FIELD_TO_MATCH &&  deleted_accounts.contains(e.getValue()))
            {
                // we found a value matching the key
                logger.info("Matching key found, deleting {}", e.getValue());
                return false;
            }
        }
        return true;

    }

    @Override
    public boolean shouldKeepCluster(OnDiskAtomIterator partition, Composite name) {
        for (ColumnDefinition def : cfs.metadata.clusteringColumns())
        {
            if ( def.name.bytes == FIELD_TO_MATCH && deleted_accounts.contains(def.name.bytes))
            {
                return false;
            }
        }
        return true;
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        logger.info("Validating AccountDeleter");

        if(!options.containsKey(DELETED_QUERY_KEY)) {
            throw new ConfigurationException("delete_records required.  should return a result with 1 field");
        }
        if(!options.containsKey("field_to_match")) {
            throw new ConfigurationException("field_to_match must be specified");
        }
        DELETED_QUERY = options.get(DELETED_QUERY_KEY);
        logger.info("Using  query: {}", DELETED_QUERY);

        options.remove(DELETED_QUERY_KEY);
        options.remove("field_to_match");

        return options;
    }
}
