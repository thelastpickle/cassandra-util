package com.protectwise.cassandra.db.compaction;

/**
 * Created by jhaddad on 12/29/16.
 */

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.exceptions.ConfigurationException;

import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.composites.Composite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

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

    private static String FIELD_TO_MATCH;


    public AccountDeleter(ColumnFamilyStore cfs, Map<String, String> options) {
        super(cfs, options);

        DELETED_QUERY = options.get(DELETED_QUERY_KEY);
        FIELD_TO_MATCH = options.get("field_to_match");

        logger.info("Using account deleter with {}", DELETED_QUERY);

        UntypedResultSet rows = QueryProcessor.executeInternal(DELETED_QUERY);
        logger.info("AccountDeleter created, {} accounts purgable.", rows.size());

        for(UntypedResultSet.Row row: rows) {
            // TODO: field is hard coded for POC, please fix
            logger.info("Row {} marked for deletion", row.getBytes("videoid"));

        }
    }

    @Override
    public boolean shouldKeepPartition(OnDiskAtomIterator key) {
//        logger.info("examining {}", key);
        return true;
    }

    @Override
    public boolean shouldKeepCluster(OnDiskAtomIterator partition, Composite name) {
//        logger.info("examining {} // {}", partition.getKey(), name);
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
