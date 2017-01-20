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

    public AccountDeleter(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);

        DELETED_QUERY = options.get(DELETED_QUERY_KEY);
        FIELD_TO_MATCH = ByteBufferUtil.bytes(options.get("field_to_match"));

        logger.info("Using account deleter with {}", DELETED_QUERY);

        UntypedResultSet rows = QueryProcessor.executeInternal(DELETED_QUERY);
        logger.info("AccountDeleter created, {} accounts purgable.", rows.size());

        deleted_accounts = new HashSet<>();

        FIELD_TO_MATCH = rows.metadata().get(0).name.bytes;
        logger.info("Field: {}", FIELD_TO_MATCH);

        String colname = rows.metadata().get(0).name.toString();
        for(UntypedResultSet.Row row: rows)
        {
            logger.info("id {} marked for deletion", row.getBytes(colname));
            deleted_accounts.add(row.getBytes(colname));
        }
    }

    @Override
    public boolean shouldKeepPartition(OnDiskAtomIterator key)
    {
        for (Map.Entry<ByteBuffer, ByteBuffer> e : this.getNamedPkColumns(key).entrySet())
        {
            // TODO make sure we're looking at the right key
            if (e.getKey().equals(FIELD_TO_MATCH) && deleted_accounts.contains(e.getValue()))
            {
                // we found a value matching the key
                logger.info("Matching key found, deleting {}", e.getValue());
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean shouldKeepCluster(OnDiskAtomIterator partition, Composite name)
    {
        for (ColumnDefinition def : cfs.metadata.clusteringColumns())
        {
            if (def.name.bytes.equals(FIELD_TO_MATCH))
            {
                ByteBuffer bytes = this.getBytes(name, def);
//                logger.info("Matched field, Data: {}", bytes.toString());
                if (deleted_accounts.contains(bytes))
                {
                    logger.info("Matching key found in shouldKeepCluster, deleting {}", bytes);
                    return false;
                }
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
