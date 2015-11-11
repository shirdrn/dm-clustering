package org.shirdrn.dm.clustering.dbscan;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.dm.clustering.common.Clustering;
import org.shirdrn.dm.clustering.common.NamedThreadFactory;
import org.shirdrn.dm.clustering.common.Point2D;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class DBSCANClustering implements Clustering {

	private static final Log LOG = LogFactory.getLog(DBSCANClustering.class);
	private double eps;
	private int minPts;
	private final EpsEstimator epsEstimator;
	private final Map<Point2D, Set<Point2D>> corePointScopeSet = Maps.newHashMap();
	private final Map<Point2D, Set<Point2D>> clusteredPoints = Maps.newHashMap();
	private final List<Point2D> noisePoints = Lists.newArrayList();
	private final CountDownLatch latch;
	private final ExecutorService executorService;
	private final int parallism;
	private final BlockingQueue<Point2D> taskQueue;
	private int clusterCount;
	
	public DBSCANClustering(int minPts, int parallism) {
		super();
		this.minPts = minPts;
		this.parallism = parallism;
		epsEstimator = new EpsEstimator(minPts, parallism);
		latch = new CountDownLatch(parallism);
		executorService = Executors.newCachedThreadPool(new NamedThreadFactory("CORE"));
		taskQueue = new LinkedBlockingQueue<Point2D>();
		LOG.info("Config: minPts=" + minPts + ", parallism=" + parallism);
	}
	
	@Override
	public void clustering(File... files) {
		minPts = 4;
		eps = epsEstimator.computeKDistance(files).estimateEps().getSelectedKDistance();
		LOG.info("Eps got: eps=" + eps);
		
		// recognize core point
		try {
			for (int i = 0; i < parallism; i++) {
				CorePointCalculator calculator = new CorePointCalculator();
				executorService.execute(calculator);
				LOG.info("Core point calculator started: " + calculator);
			}
			
			Iterator<Point2D> iter = epsEstimator.allPointIterator();
			while(iter.hasNext()) {
				Point2D p = iter.next();
				while(!taskQueue.offer(p)) {
					Thread.sleep(5);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				latch.await();
			} catch (InterruptedException e) { }
			LOG.info("Shutdown executor service: " + executorService);
			executorService.shutdown();
		}
		LOG.info("Point statistics: corePointSize=" + corePointScopeSet.keySet().size() + ", noisePointSize=" + noisePoints.size());
		
		// join connected core points
		LOG.info("Joining connected points ...");
		List<Point2D> corePoints = Lists.newArrayList(corePointScopeSet.keySet());
		while(!corePoints.isEmpty()) {
			Set<Point2D> set = Sets.newHashSet();
			Point2D p = corePoints.remove(0);
			Set<Point2D> collectedPoints = joinConnectedCorePoints(Lists.newArrayList(p), corePoints);
			while(!collectedPoints.isEmpty()) {
				corePoints.removeAll(collectedPoints);
				set.addAll(collectedPoints);
				collectedPoints = joinConnectedCorePoints(Lists.newArrayList(collectedPoints), corePoints);
			}
			clusteredPoints.put(p, set);
		}
		
		clusterCount = clusteredPoints.size();
		LOG.info("Finished clustering: clusterCount=" + clusterCount);
		
		
		displayClusters();
	}
	
	private void displayClusters() {
		// display core points
		Iterator<Entry<Point2D, Set<Point2D>>> coreIter = clusteredPoints.entrySet().iterator();
		int id = 0;
		while(coreIter.hasNext()) {
			++id;
			Entry<Point2D, Set<Point2D>> core = coreIter.next();
			Set<Point2D> set = Sets.newHashSet();
			set.add(core.getKey());
			set.addAll(corePointScopeSet.get(core.getKey()));
//			System.out.println("== CLUSTER(" + (++id) + ") == ]");
			for(Point2D p : core.getValue()) {
				set.addAll(core.getValue());
				set.addAll(corePointScopeSet.get(p));
			}
			for(Point2D ap : set) {
				System.out.println(ap + ", " + id);
			}
//			System.out.println();
			
			for(Point2D p : core.getValue()) {
				LOG.debug(corePointScopeSet.get(p));
			}
		}
		
		// display noise points
		LOG.info("== NOISE ==");
		LOG.info(noisePoints);
	}
	
	private Set<Point2D> joinConnectedCorePoints(List<Point2D> connectedPoints, List<Point2D> leftCorePoints) {
		Set<Point2D> set = Sets.newHashSet();
		for (int i = 0; i < connectedPoints.size(); i++) {
			Point2D p1 = connectedPoints.get(i);
			for (int j = 0; j < leftCorePoints.size(); j++) {
				Point2D p2 = leftCorePoints.get(j);
				double distance = epsEstimator.getDistance(Sets.newHashSet(p1, p2));
				if(distance <= eps) {
					// join 2 core points to the same cluster
					set.add(p2);
				}
			}
		}
		return set;
	}
	
	@Override
	public int getClusteredCount() {
		return clusterCount;
	}
	
	/**
	 * Compute core point and points in this scope
	 *
	 * @author yanjun
	 */
	class CorePointCalculator extends Thread {
		
		private final Log LOG = LogFactory.getLog(CorePointCalculator.class);
		private int processedPoints;
		
		@Override
		public void run() {
			try {
				Thread.sleep(1000);
				while(!taskQueue.isEmpty()) {
					Point2D p1 = taskQueue.poll();
					if(p1 != null) {
						++processedPoints;
						Set<Point2D> set = Sets.newHashSet();
						Iterator<Point2D> iter = epsEstimator.allPointIterator();
						while(iter.hasNext()) {
							Point2D p2 = iter.next();
							if(!p2.equals(p1)) {
								double distance = epsEstimator.getDistance(Sets.newHashSet(p1, p2));
								// collect a point belonging to the point p1
								if(distance <= eps) {
									set.add(p2);
								}
							}
						}
						// decide whether p1 is core point
						if(set.size() >= minPts) {
							corePointScopeSet.put(p1, set);
							LOG.debug("Decide core point: point" + p1 + ", set=" + set);
						} else {
							noisePoints.add(p1);
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				latch.countDown();
				LOG.info("Calculator exit, STAT: [id=" + this + ", processedPoints=" + processedPoints + "]");
			}
		}
	}
	
	public static void main(String[] args) {
		Clustering c = new DBSCANClustering(4, 5);
		c.clustering(new File("C:\\Users\\yanjun\\Desktop\\xy_zfmx.txt"));
	}

}
