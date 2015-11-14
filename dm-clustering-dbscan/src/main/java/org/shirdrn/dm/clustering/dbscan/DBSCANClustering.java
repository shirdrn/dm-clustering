package org.shirdrn.dm.clustering.dbscan;

import java.io.File;
import java.util.Iterator;
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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class DBSCANClustering implements Clustering {

	private static final Log LOG = LogFactory.getLog(DBSCANClustering.class);
	private double eps;
	private int minPts;
	private final EpsEstimator epsEstimator;
	private final Map<Point2D, Set<Point2D>> corePointScopeSet = Maps.newHashMap();
	private final Map<Point2D, Set<Point2D>> clusteredPoints = Maps.newHashMap();
	private final Set<Point2D> noisePoints = Sets.newHashSet();
	private final CountDownLatch latch;
	private final ExecutorService executorService;
	private final int parallism;
	private final BlockingQueue<Point2D> taskQueue;
	private volatile boolean completed = false;
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
	
	public void generateSortedKDistances(File... files) {
		epsEstimator.computeKDistance(files).estimateEps();
	}
	
	@Override
	public void clustering() {
		// recognize core points
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
					Thread.sleep(10);
				}
				LOG.debug("Added to taskQueue: " + p);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				completed = true;
				latch.await();
			} catch (InterruptedException e) { }
			LOG.info("Shutdown executor service: " + executorService);
			executorService.shutdown();
		}
		LOG.info("Point statistics: corePointSize=" + corePointScopeSet.keySet().size());
		
		// join connected core points
		LOG.info("Joining connected core points ...");
		Set<Point2D> corePoints = Sets.newHashSet(corePointScopeSet.keySet());
		while(true) {
			Set<Point2D> set = Sets.newHashSet();
			Iterator<Point2D> iter = corePoints.iterator();
			if(iter.hasNext()) {
				Point2D p = iter.next();
				iter.remove();
				Set<Point2D> connectedPoints = joinConnectedCorePoints(p, corePoints);
				set.addAll(connectedPoints);
				while(!connectedPoints.isEmpty()) {
					connectedPoints = joinConnectedCorePoints(connectedPoints, corePoints);
					set.addAll(connectedPoints);
				}
				clusteredPoints.put(p, set);
			} else {
				break;
			}
		}
		clusterCount = clusteredPoints.size();
		LOG.info("Connected core points computed.");
		
		// process noise points
		Iterator<Point2D> iter = noisePoints.iterator();
		while(iter.hasNext()) {
			Point2D np = iter.next();
			if(corePointScopeSet.containsKey(np)) {
				iter.remove();
			} else {
				for(Set<Point2D> set : corePointScopeSet.values()) {
					if(set.contains(np)) {
						iter.remove();
						break;
					}
				}
			}
		}
		
		LOG.info("Finished clustering: clusterCount=" + clusterCount + ", noisePointCount=" + noisePoints.size());
		
		displayClusters();
	}
	
	private Set<Point2D> joinConnectedCorePoints(Set<Point2D> connectedPoints, Set<Point2D> leftCorePoints) {
		Set<Point2D> set = Sets.newHashSet();
		for(Point2D p1 : connectedPoints) {
			set.addAll(joinConnectedCorePoints(p1, leftCorePoints));
		}
		return set;
	}
	
	private Set<Point2D> joinConnectedCorePoints(Point2D p1, Set<Point2D> leftCorePoints) {
		Set<Point2D> set = Sets.newHashSet();
		for(Point2D p2 : leftCorePoints) {
			double distance = epsEstimator.getDistance(Sets.newHashSet(p1, p2));
			if(distance <= eps) {
				// join 2 core points to the same cluster
				set.add(p2);
			}
		}
		// remove connected points
		leftCorePoints.removeAll(set);
		return set;
	}
	
	public void setMinPts(int minPts) {
		this.minPts = minPts;
	}

	private void displayClusters() {
		// display noise points
		int noiseClusterId = -1;
		for(Point2D p : noisePoints) {
			System.out.println(p.getX() + "," + p.getY() + "," + noiseClusterId);
		}
		
		// display core points
		Iterator<Entry<Point2D, Set<Point2D>>> coreIter = clusteredPoints.entrySet().iterator();
		int id = 0;
		while(coreIter.hasNext()) {
			++id;
			Entry<Point2D, Set<Point2D>> core = coreIter.next();
			Set<Point2D> set = Sets.newHashSet();
			set.add(core.getKey());
			set.addAll(corePointScopeSet.get(core.getKey()));
			for(Point2D p : core.getValue()) {
				set.addAll(core.getValue());
				set.addAll(corePointScopeSet.get(p));
			}
			for(Point2D p : set) {
				System.out.println(p.getX() + "," + p.getY() + "," + id);
			}
		}
		
	}
	
	@Override
	public int getClusteredCount() {
		return clusterCount;
	}
	
	public void setEps(double eps) {
		this.eps = eps;
	}
	
	/**
	 * Compute core point and points in this scope
	 *
	 * @author yanjun
	 */
	private final class CorePointCalculator extends Thread {
		
		private final Log LOG = LogFactory.getLog(CorePointCalculator.class);
		private int processedPoints;
		
		@Override
		public void run() {
			try {
				Thread.sleep(1000);
				while(!completed || !taskQueue.isEmpty()) {
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
							// here, perhaps a point was wrongly put into noise point set
							// afterwards we should remedy noise point set
							if(!noisePoints.contains(p1)) {
								noisePoints.add(p1);
							}
						}
					} else {
						Thread.sleep(50);
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
	
	public EpsEstimator getEpsEstimator() {
		return epsEstimator;
	}
	
	public static void main(String[] args) {
		// generate sorted k-distances sequences
//		int minPts = 4;
		int minPts = 8;
		DBSCANClustering c = new DBSCANClustering(minPts, 8);
		c.getEpsEstimator().setOutoutKDsitance(false);
		c.generateSortedKDistances(new File("C:\\Users\\yanjun\\Desktop\\xy_zfmx.txt"));
		
		// execute clustering procedure
//		double eps = 0.0025094814205335555;
//		double eps = 0.004417483559674606;
//		double eps = 0.006147849217403014;
		
		
//		double eps = 0.004900098978598581;
//		double eps = 0.009566439044911;
		double eps = 0.013621050253196359;


		c.setEps(eps);
		c.setMinPts(4);
		c.clustering();
	}

}
