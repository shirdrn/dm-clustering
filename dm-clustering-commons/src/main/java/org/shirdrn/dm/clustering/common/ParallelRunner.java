package org.shirdrn.dm.clustering.common;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Lists;

public abstract class ParallelRunner {

	private static final Log LOG = LogFactory.getLog(ParallelRunner.class);
	private final int parallism;
	protected final CountDownLatch latch;
	protected final ExecutorService executorService;
	private final List<? extends Thread> workers = Lists.newArrayList();
	private int calculatorCount;
	private int taskIndex = 0;
	
	public ParallelRunner(String poolName, int parallism) {
		this.parallism = parallism;
		latch = new CountDownLatch(this.parallism);
		executorService = Executors.newCachedThreadPool(new NamedThreadFactory(poolName));
		LOG.info("Config: parallism=" + parallism);
	}
	
	@SuppressWarnings("unchecked")
	protected <T> T getWorker(Class<T> workerClass) {
		int index = taskIndex++ % calculatorCount;
		return (T) workers.get(index);
	}
}
