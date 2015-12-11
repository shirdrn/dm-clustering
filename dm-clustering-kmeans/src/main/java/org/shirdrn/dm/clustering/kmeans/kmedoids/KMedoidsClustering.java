package org.shirdrn.dm.clustering.kmeans.kmedoids;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.dm.clustering.common.CenterPoint;
import org.shirdrn.dm.clustering.common.ClusterPoint;
import org.shirdrn.dm.clustering.common.ClusterPoint2D;
import org.shirdrn.dm.clustering.common.ClusteringResult;
import org.shirdrn.dm.clustering.common.DistanceCache;
import org.shirdrn.dm.clustering.common.NamedThreadFactory;
import org.shirdrn.dm.clustering.common.Point2D;
import org.shirdrn.dm.clustering.common.utils.ClusteringUtils;
import org.shirdrn.dm.clustering.common.utils.FileUtils;
import org.shirdrn.dm.clustering.kmeans.common.AbstractKMeansClustering;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Basic k-medoids clustering algorithm. 
 *
 * @author yanjun
 */
public class KMedoidsClustering extends AbstractKMeansClustering {

	private static final Log LOG = LogFactory.getLog(KMedoidsClustering.class);
	private final List<NearestMedoidSeeker> seekers = Lists.newArrayList();
	private int taskIndex = 0;
	private final int seekerQueueSize = 200;
	private CountDownLatch latch;
	private final ExecutorService executorService;
	private volatile boolean completeToAssignTask = false;
	private final Random random = new Random();
	private final DistanceCache distanceCache;
	private volatile boolean finallyCompleted = false;
	private final Object signalLock = new Object();
	
	public KMedoidsClustering(int k, int maxIterations, int parallism) {
		super(k, maxIterations, parallism);
		distanceCache = new DistanceCache(Integer.MAX_VALUE);
		executorService = Executors.newCachedThreadPool(new NamedThreadFactory("SEEKER"));
		latch = new CountDownLatch(parallism);
	}
	
	@Override
	public void clustering() {
		// parse sample files
		FileUtils.read2DPointsFromFiles(allPoints, "[\t,;\\s]+", inputFiles);
		LOG.info("Total points: count=" + allPoints.size());
		
		ClusterHolder currentHolder = new ClusterHolder();
		ClusterHolder previousHolder = null;
		
		currentHolder.medoids = initialCentroidsSelectionPolicy.select(k, allPoints);
		LOG.info("Initial selected medoids: " + currentHolder.medoids);
		
		// start seeker threads
		for (int i = 0; i < parallism; i++) {
			final NearestMedoidSeeker seeker = new NearestMedoidSeeker(seekerQueueSize);
			executorService.execute(seeker);
			seekers.add(seeker);
		}
		
		// /////////////////
		// make iterations
		// /////////////////
		
		boolean firstTimeToAssign = true;
		int numIterations = 0;
		double previousSAD = 0.0;
		double currentSAD = 0.0;
		try {
			while(!finallyCompleted) {
				try {
					LOG.debug("Current medoid set: " + currentHolder.medoids);
					if(firstTimeToAssign) {
						assignNearestMedoids(currentHolder, true);
						firstTimeToAssign = false;
					} else {
						assignNearestMedoids(currentHolder, false);
					}
					
					// merge result
					mergeMedoidAssignedResult(currentHolder);
					LOG.debug("Merged result: " + currentHolder.medoidWithNearestPointSet);
					
					// compare cost for 2 iterations, we use SAD (sum of absolute differences)
					if(previousSAD == 0.0) {
						// first time compute SAD
						previousSAD = currentSAD;
						currentSAD = computeSAD(currentHolder);
					} else {
						// compute current cost when using random point to substitute for the medoid
						currentSAD = computeSAD(currentHolder);
						// compare SADs
						if(currentSAD - previousSAD < 0.0) {
							previousHolder = currentHolder;
							previousSAD = currentSAD;
						}
						
						RandomPoint randomPoint = selectNonCenterPointRandomly(currentHolder);
						LOG.debug("Randomly selected: " + randomPoint);
						
						// construct new cluster holder
						currentHolder = constructNewHolder(currentHolder, randomPoint);
					}
					LOG.info("Iteration #" + (++numIterations) + ": previousSAD=" + previousSAD + ", currentSAD=" + currentSAD);
					
					if(numIterations > maxIterations) {
						finallyCompleted = true;
					}
				} catch(Exception e) {
					Throwables.propagate(e);
				} finally {
					try {
						if(!finallyCompleted) {
							latch = new CountDownLatch(parallism);
							completeToAssignTask = false;
						}
						Thread.sleep(10);
						synchronized(signalLock) {
							signalLock.notifyAll();
						}
					} catch (InterruptedException e) {}
				}
			}
		} finally {
			LOG.info("Shutdown executor service: " + executorService);
			executorService.shutdown();
		}
		
		// finally result
		centerPointSet.addAll(previousHolder.medoids);
		Iterator<Entry<CenterPoint, List<Point2D>>> iter = previousHolder.medoidWithNearestPointSet.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<CenterPoint, List<Point2D>> entry = iter.next();
			int clusterId = entry.getKey().getId();
			Set<ClusterPoint<Point2D>> set = Sets.newHashSet();
			for(Point2D p : entry.getValue()) {
				set.add(new ClusterPoint2D(p, clusterId));
			}
			clusteredPoints.put(clusterId, set);
		}
	}

	private void mergeMedoidAssignedResult(ClusterHolder currentHolder) {
		currentHolder.medoidWithNearestPointSet = Maps.newTreeMap();
		for(NearestMedoidSeeker seeker : seekers) {
			LOG.debug("seeker.clusteringNearestPoints: " + seeker.clusteringNearestPoints);
			Iterator<Entry<CenterPoint, List<Point2D>>> iter = seeker.clusteringNearestPoints.entrySet().iterator();
			while(iter.hasNext()) {
				Entry<CenterPoint, List<Point2D>> entry = iter.next();
				List<Point2D> set = currentHolder.medoidWithNearestPointSet.get(entry.getKey());
				if(set == null) {
					set = Lists.newArrayList();
					currentHolder.medoidWithNearestPointSet.put(entry.getKey(), set);
				}
				set.addAll(entry.getValue());
			}
		}
	}

	private void assignNearestMedoids(final ClusterHolder holder, boolean firstTimeToAssign) {
		LOG.debug("firstTimeToAssign=" + firstTimeToAssign);
		try {
			// assign tasks to seeker threads
			if(firstTimeToAssign) {
				holder.centerPoints = Sets.newHashSet();
				for(CenterPoint medoid : holder.medoids) {
					holder.centerPoints.add(medoid.toPoint());
				}
				LOG.debug("holder.centerPoints: " + holder.centerPoints);
				
				for(Point2D p : allPoints) {
					LOG.debug("Assign point: " + p);
					if(!holder.centerPoints.contains(p)) {
						selectSeeker().q.put(new Task(holder.medoids, p));
					}
				}
			} else {
				for(List<Point2D> points : holder.medoidWithNearestPointSet.values()) {
					for(Point2D p : points) {
						selectSeeker().q.put(new Task(holder.medoids, p));
					}
				}
			}
		} catch(Exception e) {
			Throwables.propagate(e);
		} finally {
			try {
				completeToAssignTask = true;
				latch.await();
			} catch (InterruptedException e) { }
		}
	}

	private ClusterHolder constructNewHolder(final ClusterHolder holder, RandomPoint randomPoint) {
		ClusterHolder newHolder = new ClusterHolder();
		
		// collect center points with type Point2D for a holder object
		// from previous result of clustering procedure
		newHolder.centerPoints = Sets.newHashSet();
		for(CenterPoint c : holder.medoidWithNearestPointSet.keySet()) {
			newHolder.centerPoints.add(c.toPoint());
		}
		
		Point2D newPoint = randomPoint.point;
		CenterPoint oldMedoid = randomPoint.medoid;
		
		// create a new center point with type CenterPoint based on the randomly selected non-medoid point
		// and it's id is equal to the old medoid's
		CenterPoint newMedoid = new CenterPoint(oldMedoid.getId(), newPoint);
		
		// use new medoid above to substitute the old medoid
		newHolder.centerPoints.remove(oldMedoid.toPoint());
		newHolder.centerPoints.add(newPoint);
 		
		newHolder.medoids = Sets.newTreeSet();
		newHolder.medoids.addAll(holder.medoidWithNearestPointSet.keySet());
		newHolder.medoids.remove(oldMedoid); // remove old medoid from center point set of new holder object
		newHolder.medoids.add(newMedoid);
		
		// copy the holder's medoidWithNearestPointSet, and modify it
		newHolder.medoidWithNearestPointSet = Maps.newTreeMap();
		newHolder.medoidWithNearestPointSet.putAll(holder.medoidWithNearestPointSet);
		List<Point2D> oldPoints = newHolder.medoidWithNearestPointSet.get(oldMedoid);
		oldPoints.remove(newPoint); // remove new randomly selected non-medoid point from previous result set of clustering
		oldPoints.add(oldMedoid.toPoint()); // add old medoid point to the non-medoid set
		newHolder.medoidWithNearestPointSet.put(newMedoid, oldPoints);
		return newHolder;
	}
	
	private double computeSAD(final ClusterHolder holder) {
		double sad = 0.0; 
		for(CenterPoint medoid : holder.medoidWithNearestPointSet.keySet()) {
			double distances = 0.0;
			List<Point2D> points = holder.medoidWithNearestPointSet.get(medoid);
			for(Point2D p : points) {
				distances += distanceCache.computeDistance(medoid.toPoint(), p);
			}
			sad += distances;
		}
		return sad;
	}
	
	private RandomPoint selectNonCenterPointRandomly(ClusterHolder holder) {
		List<CenterPoint> medoids = new ArrayList<CenterPoint>(holder.medoidWithNearestPointSet.keySet());
		CenterPoint selectedMedoid = medoids.get(random.nextInt(medoids.size()));
		
		List<Point2D> belongingPoints = holder.medoidWithNearestPointSet.get(selectedMedoid);
		Point2D point = belongingPoints.get(random.nextInt(belongingPoints.size()));
		return new RandomPoint(selectedMedoid, point);
	}
	
	private class Task {
		
		final TreeSet<CenterPoint> medoids;
		final Point2D point;
		
		public Task(TreeSet<CenterPoint> medoids, Point2D point) {
			super();
			this.medoids = medoids;
			this.point = point;
		}
	}
	
	private class ClusterHolder {
		
		/** snapshot of clustering result: medoids of clustering result, as well as non-medoid points */
		private TreeMap<CenterPoint, List<Point2D>> medoidWithNearestPointSet;
		/** center point set represented by Point2D */
		private Set<Point2D> centerPoints; 
		/** center point set represented by CenterPoint */
		private TreeSet<CenterPoint> medoids;
		
		public ClusterHolder() {
			super();
		}
	}
	
	private class RandomPoint {
		/** medoid which the random point belongs to */
		private final CenterPoint medoid;
		/** a non-medoid point selected randomly */
		private final Point2D point;
		
		public RandomPoint(CenterPoint medoid, Point2D point) {
			super();
			this.medoid = medoid;
			this.point = point;
		}
		
		@Override
		public String toString() {
			return "RandomPoint[medoid=" + medoid + ", point=" + point + "]";
		}
	}

	private NearestMedoidSeeker selectSeeker() {
		int index = taskIndex++ % parallism;
		return seekers.get(index);
	}
	
	private class NearestMedoidSeeker implements Runnable {
		
		private final Log LOG = LogFactory.getLog(NearestMedoidSeeker.class);
		private final BlockingQueue<Task> q;
		private Map<CenterPoint, List<Point2D>> clusteringNearestPoints = Maps.newHashMap();
		private int processedTasks = 0;
		
		public NearestMedoidSeeker(int qsize) {
			q = new LinkedBlockingQueue<Task>(qsize);
		}
		
		@Override
		public void run() {
			while(!finallyCompleted) {
				try {
					assign();
					Thread.sleep(200);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		private void assign() throws InterruptedException {
			try {
				LOG.debug("Q size: " + q.size());
				while(!(q.isEmpty() && completeToAssignTask)) {
					processedTasks++;
					final Task task = q.poll();
					if(task != null) {
						final Point2D p1 = task.point;
						double minDistance = Double.MAX_VALUE;
						CenterPoint nearestMedoid = null;
						for(CenterPoint medoid : task.medoids) {
							final Point2D p2 = medoid.toPoint();
							Double distance = distanceCache.computeDistance(p1, p2);
							if(distance < minDistance) {
								minDistance = distance;
								nearestMedoid = medoid;
							}
						}
						LOG.debug("Nearest medoid seeked: point=" + p1 + ", medoid=" + nearestMedoid);
						
						List<Point2D> points = clusteringNearestPoints.get(nearestMedoid);
						if(points == null) {
							points = Lists.newArrayList();
							clusteringNearestPoints.put(nearestMedoid, points);
						}
						points.add(p1);
					} else {
						Thread.sleep(150);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				latch.countDown();
				LOG.debug("Point processed: processedTasks=" + processedTasks);
				
				synchronized(signalLock) {
					signalLock.wait();
				}
				
				clusteringNearestPoints = Maps.newHashMap();
				processedTasks = 0;
			}
		}
	}
	
	public static void main(String[] args) {
		int k = 10;
		int parallism = 4;
		int maxIterations = 1000;
		KMedoidsClustering c = new KMedoidsClustering(k, maxIterations, parallism);
		File dir = FileUtils.getDataRootDir();
		c.setInputFiles(new File(dir, "points.txt"));
		c.clustering();
		
		System.out.println("== Clustered points ==");
		ClusteringResult<Point2D> result = c.getClusteringResult();
		ClusteringUtils.print2DClusterPoints(result.getClusteredPoints());
		
		// print medoids
		System.out.println("== Medoid points ==");
		for(CenterPoint p : c.getCenterPointSet()) {
			System.out.println(p.getX() + "," + p.getY() + "," + p.getId());
		}
	}

}
