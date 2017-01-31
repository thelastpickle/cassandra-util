/*
 * Copyright 2016 ProtectWise, Inc.
 *
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
package com.protectwise.cassandra.db.compaction;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.service.ActiveRepairService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class BackupSinkForDeletingCompaction implements IDeletedRecordsSink
{
	private static final Logger logger = LoggerFactory.getLogger(BackupSinkForDeletingCompaction.class);

	protected final ColumnFamilyStore cfs;
	protected final ColumnFamily columnFamily;
	protected final File targetDirectory;
	protected final long keysPerSSTable;

	protected SSTableWriter writer;
	protected DecoratedKey currentKey;
	protected long numCells = 0;
	protected long numKeys = 0;

	public BackupSinkForDeletingCompaction(ColumnFamilyStore cfs, File targetDirectory)
	{
		// TODO: Wow, this key estimate is probably grossly over-estimated...  Not sure how to get a better one here.
		// Need to check if this keyEstimate divisor is correct. Unsure if we should be using ColumnFamilyMetrics here
		// to get the live SSTable count. The problem is the 'this' object identifier needs to be the first call in the
		// constructor which means we are unable instance of the ColumnFamilyMetrics class prior to the call. For now
		// we will reach into the ColumnFamilyStore class and obtain the value from the tracker which is what the
		// ColumnFamilyMetrics class does.
		this(cfs, targetDirectory, cfs.estimateKeys() / cfs.getTracker().getSSTables().size());
	}

	public BackupSinkForDeletingCompaction(ColumnFamilyStore cfs, File targetDirectory, long keyEstimate)
	{
		this.cfs = cfs;
		this.targetDirectory = targetDirectory;
		this.keysPerSSTable = keyEstimate;

		// Right now we're just doing one sink per compacted sstable, so they'll be pre-sorted, meaning
		// we don't need to bother resorting the data.
		columnFamily = ArrayBackedSortedColumns.factory.create(cfs.keyspace.getName(), cfs.getColumnFamilyName());
	}

	protected void flush()
	{
		if (!columnFamily.isEmpty())
		{
			writer.append(currentKey, columnFamily);
			columnFamily.clear();
		}
	}

	@Override
	public void accept(OnDiskAtomIterator partition)
	{
		flush();
		currentKey = partition.getKey();
		numKeys++;
		// Write through the entire partition.
		while (partition.hasNext())
		{
			accept(partition.getKey(), partition.next());
		}
	}

	@Override
	public void accept(DecoratedKey key, OnDiskAtom cell)
	{
		if (currentKey != key)
		{
			flush();
			numKeys++;
			currentKey = key;
		}

		numCells++;
		columnFamily.addAtom(cell);
	}

	@Override
	public void begin()
	{
		writer = SSTableWriter.create(cfs.getTempSSTablePath(targetDirectory), keysPerSSTable, ActiveRepairService.UNREPAIRED_SSTABLE);
		logger.info("Opening backup writer for {}", writer.getFilename());
	}

	@Override
	public void close() throws IOException
	{
		if (numKeys > 0 && numCells > 0)
		{
			flush();
			logger.info("Cleanly closing backup operation for {} with {} keys and {} cells", writer.getFilename(), numKeys, numCells);
			writer.close();
		}
		else
		{
			// If deletion convicted nothing, then don't bother writing an empty backup file.
			abort();
		}
	}

	/**
	 * Abort the operation and discard any outstanding data.
	 * Only one of close() or abort() should be called.
	 */
	@Override
	public void abort()
	{
		logger.info("Aborting backup operation for {}", writer.getFilename());
		columnFamily.clear();
		writer.abort();
	}
}
