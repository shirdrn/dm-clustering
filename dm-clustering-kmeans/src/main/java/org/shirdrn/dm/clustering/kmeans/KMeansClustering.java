package org.shirdrn.dm.clustering.kmeans;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
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
import org.shirdrn.dm.clustering.common.NamedThreadFactory;
import org.shirdrn.dm.clustering.common.Point2D;
import org.shirdrn.dm.clustering.common.utils.ClusteringUtils;
import org.shirdrn.dm.clustering.common.utils.FileUtils;
import org.shirdrn.dm.clustering.common.utils.MetricUtils;
import org.shirdrn.dm.clustering.kmeans.common.AbstractKMeansClustering;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import com.google.common.collect.Sets;

/**
 * Basic k-means clustering algorithm.
 *
 * @author yanjun
 */
public class KMeansClustering extends AbstractKMeansClustering {

	private static final Log LOG = LogFactory.getLog(KMeansClustering.class);
	private final ExecutorService executorService;
	private CountDownLatch latch;
	private int taskIndex = 0;
	private int calculatorQueueSize = 200;
	private final List<CentroidCalculator> calculators = Lists.newArrayList();
	private volatile boolean completeToAssignTask = false;
	private volatile boolean clusteringCompletedFinally = false;
	
	public KMeansClustering(int k, float maxMovingPointRate, int maxIterations, int parallism) {
		super(k, maxMovingPointRate, maxIterations, parallism);
		latch = new CountDownLatch(parallism);
		executorService = Executors.newCachedThreadPool(new NamedThreadFactory("CENTROID"));
	}
	
	public void initialize(Collection<Point2D> points) {
		if(points == null) {
			// parse sample files
			FileUtils.read2DPointsFromFiles(allPoints, "[\t,;\\s]+", inputFiles);
			LOG.info("Total points: count=" + allPoints.size());
		} else {
			allPoints.addAll(points);
		}
	}
	
	public void initialize() {
		initialize(null);
	}
	
	@Override
	public void clustering() {		
		// start centroid calculators
		for (int i = 0; i < parallism; i++) {
			CentroidCalculator calculator = new CentroidCalculator(calculatorQueueSize);
			calculators.add(calculator);
			executorService.execute(calculator);
			LOG.debug("Centroid calculator started: " + calculator);
		}
		
		// sort by centroid id ASC
		TreeSet<CenterPoint> centroids = initialCentroidsSelectionPolicy.select(k, allPoints);
		LOG.debug("Initial selected centroids: " + centroids);
		
		int iterations = 0;
		boolean stopped = false;
		CentroidSetWithClusteringPoints lastClusteringResult = null;
		CentroidSetWithClusteringPoints currentClusteringResult = null;
		int totalPointCount = allPoints.size();
		float currentClusterMovingPointRate = 1.0f;
		try {
			// enter clustering iteration procedure
			while(currentClusterMovingPointRate > maxMovingPointRate 
					&& !stopped 
					&& iterations < maxIterations) {
				LOG.info("START iterate: #" + (++iterations));
				
				currentClusteringResult = computeCentroids(centroids);
				LOG.debug("Re-computed centroids: " + centroids);
				
				// compute centroid convergence status
				int numMovingPoints = 0;
				if(lastClusteringResult == null) {
					numMovingPoints = totalPointCount;
				} else {
					// compare 2 iterations' result for centroid computation
					numMovingPoints = analyzeMovingPoints(lastClusteringResult.clusteringPoints, currentClusteringResult.clusteringPoints);
					
					// check iteration stop condition
					boolean isIdentical = (currentClusteringResult.centroids.size() ==
							Multisets.intersection(HashMultiset.create(lastClusteringResult.centroids), HashMultiset.create(currentClusteringResult.centroids)).size());
					if(iterations > 1 && isIdentical) {
						stopped = true;
					}
				}
				lastClusteringResult = currentClusteringResult;
				centroids = currentClusteringResult.centroids;
				currentClusterMovingPointRate = (float) numMovingPoints / totalPointCount;
				
				LOG.info("FINISH iterate: #" + iterations + ", k=" + k + 
						", numMovingPoints=" + numMovingPoints + 
						", totalPointCount=" + totalPointCount +
						", stopped=" + stopped +
						", currentClusterMovingPointRate=" + currentClusterMovingPointRate );
				
				// reset some structures
				reset();
				for(CentroidCalculator calculator : calculators) {
					calculator.reset();
				}
			}
		} finally {
			// notify all calculators to exit normally
			clusteringCompletedFinally = true;
			
			LOG.info("Shutdown executor service: " + executorService);
			executorService.shutdown();
			
			// process final clustering result
			LOG.info("Final clustering result: ");
			Iterator<Entry<CenterPoint, Multiset<Point2D>>> iter = currentClusteringResult.clusteringPoints.entrySet().iterator();
			while(iter.hasNext()) {
				Entry<CenterPoint, Multiset<Point2D>> entry = iter.next();
				int id = entry.getKey().getId();
				Set<ClusterPoint<Point2D>> set = Sets.newHashSet();
				for(Point2D p : entry.getValue()) {
					set.add(new ClusterPoint2D(p, id));
				}
				clusteredPoints.put(id, set);
				id++;
			}
			
			// compute centroid set
			centerPointSet.addAll(currentClusteringResult.clusteringPoints.keySet());
		}
	}

	private int analyzeMovingPoints(TreeMap<CenterPoint, Multiset<Point2D>> lastClusteringPoints,
			TreeMap<CenterPoint, Multiset<Point2D>> currentClusteringPoints) {
		// Map<current, Map<last, intersected point count>>
		Set<Point2D> movingPoints = Sets.newHashSet();
		Iterator<Entry<CenterPoint, Multiset<Point2D>>> lastIter = lastClusteringPoints.entrySet().iterator();
		Iterator<Entry<CenterPoint, Multiset<Point2D>>> currentIter = currentClusteringPoints.entrySet().iterator();
		while(lastIter.hasNext() && currentIter.hasNext()) {
			Entry<CenterPoint, Multiset<Point2D>> last = lastIter.next();
			Entry<CenterPoint, Multiset<Point2D>> current = currentIter.next();
			Multiset<Point2D> intersection = Multisets.intersection(last.getValue(), current.getValue());
			movingPoints.addAll(Multisets.difference(last.getValue(), intersection));
			movingPoints.addAll(Multisets.difference(current.getValue(), intersection));
		}
		return movingPoints.size();
	}

	private CentroidSetWithClusteringPoints computeCentroids(Set<CenterPoint> centroids) {
		try {
			for(Point2D p : allPoints) {
				CentroidCalculator calculator = getCalculator();
				calculator.q.put(new Task(p, centroids));
			}
			completeToAssignTask = true;
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			try {
				latch.await();
			} catch (InterruptedException e) { }
		}
		
		// merge clustered points, and group by centroid
		TreeMap<CenterPoint, Multiset<Point2D>> clusteringPoints = Maps.newTreeMap();
		for(CentroidCalculator calculator : calculators) {
			for(CenterPoint centroid : calculator.localClusteredPoints.keySet()) {
				Multiset<Point2D> globalPoints = clusteringPoints.get(centroid);
				if(globalPoints == null) {
					globalPoints = HashMultiset.create();
					clusteringPoints.put(centroid, globalPoints);
				}
				globalPoints.addAll(calculator.localClusteredPoints.get(centroid));
			}
		}
		
		// re-compute centroids
		TreeSet<CenterPoint> newCentroids = Sets.newTreeSet();
		Iterator<Entry<CenterPoint, Multiset<Point2D>>> iter = clusteringPoints.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<CenterPoint, Multiset<Point2D>> entry = iter.next();
			Point2D point = MetricUtils.meanCentroid(entry.getValue());
			newCentroids.add(new CenterPoint(entry.getKey().getId(), point));
		}
		return new CentroidSetWithClusteringPoints(newCentroids, clusteringPoints);
	}
	
	private class CentroidSetWithClusteringPoints {
		
		private final TreeSet<CenterPoint> centroids;
		private final TreeMap<CenterPoint, Multiset<Point2D>> clusteringPoints;
		
		public CentroidSetWithClusteringPoints(TreeSet<CenterPoint> centroids, TreeMap<CenterPoint, Multiset<Point2D>> clusteringPoints) {
			super();
			this.centroids = centroids;
			this.clusteringPoints = clusteringPoints;
		}
	}
	
	public void reset() {
		latch = new CountDownLatch(parallism);
		completeToAssignTask = false;
		taskIndex = 0;
	}
	
	private CentroidCalculator getCalculator() {
		int index = taskIndex++ % parallism;
		return calculators.get(index);
	}
	
	private final class CentroidCalculator implements Runnable {
		
		private final Log LOG = LogFactory.getLog(CentroidCalculator.class);
		private final BlockingQueue<Task> q;
		// TreeMap<centroid, points belonging to this centroid>
		private TreeMap<CenterPoint, Multiset<Point2D>> localClusteredPoints = Maps.newTreeMap();
		private int processedTasks;
		private int accumulatedProcessedTasks;
		
		public CentroidCalculator(int qsize) {
			q = new LinkedBlockingQueue<Task>(qsize);
		}
		
		@Override
		public void run() {
			while(!clusteringCompletedFinally) {
				try {
					process();
					Thread.sleep(100);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			LOG.debug("Received finally notification, exited. ");
		}

		private void process() {
			try {
				while(!q.isEmpty() || !completeToAssignTask) {
					while(!q.isEmpty()) {
						try {
							processedTasks++;
							Task task = q.poll();
							Point2D p1 = task.point;
							
							// assign points to a nearest centroid
							Distance minDistance = null;
							for(CenterPoint centroid : task.centroids) {
								double distance = MetricUtils.euclideanDistance(p1, centroid);
								if(minDistance != null) {
									if(distance < minDistance.distance) {
										minDistance = new Distance(p1, centroid, distance);
									}
								} else {
									minDistance = new Distance(p1, centroid, distance);
								}
							}
							LOG.debug("Assign Point2D[" + p1 + "] to Centroid[" + minDistance.centroid + "]"); 
							
							Multiset<Point2D> pointsBelongingToCentroid = localClusteredPoints.get(minDistance.centroid);
							if(pointsBelongingToCentroid == null) {
								pointsBelongingToCentroid = HashMultiset.create();
								localClusteredPoints.put(minDistance.centroid, pointsBelongingToCentroid);
							}
							pointsBelongingToCentroid.add(p1);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) { }
				}
			} finally {
				accumulatedProcessedTasks += processedTasks;
				latch.countDown();
				LOG.debug("Calculator finished: " + "processedTasks=" + processedTasks + 
						", accumulatedProcessedTasks=" + accumulatedProcessedTasks);
			}
		}
		
		public void reset() {
			localClusteredPoints = null;
			localClusteredPoints = Maps.newTreeMap();
			processedTasks = 0;
		}
	}
	
	private class Task {
		
		protected final Point2D point;
		protected final Set<CenterPoint> centroids;
		
		public Task(Point2D point, Set<CenterPoint> centroids) {
			super();
			this.point = point;
			this.centroids = centroids;
		}
		
		@Override
		public int hashCode() {
			return point.hashCode();
		}
		
		@Override
		public boolean equals(Object obj) {
			Task other = (Task) obj;
			return point.equals(other.point);
		}
	}
	
	private class Distance implements Comparable<Distance> {
		
		@SuppressWarnings("unused")
		private final Point2D point;
		private final CenterPoint centroid;
		private double distance = 0.0;
		
		public Distance(Point2D point, CenterPoint centroid) {
			this.point = point;
			this.centroid = centroid;
		}
		
		public Distance(Point2D point, CenterPoint centroid, double distance) {
			this(point, centroid);
			this.distance = distance;
		}

		@Override
		public int compareTo(Distance o) {
			double diff = this.distance - o.distance;
			return diff<0 ? -1 : (diff>0 ? 1 : 0);
		}
	}
	
	public static void main(String[] args) {
		int k = 10;
		float maxMovingPointRate = 0.01f;
		int maxInterations = 50;
		int parallism = 5;
		KMeansClustering c = new KMeansClustering(k, maxMovingPointRate, maxInterations, parallism);
		// set InitialCentroidsSelectionPolicy
//		c.setInitialCentroidsSelectionPolicy(new KMeansPlusPlusInitialCenterPointsSelectionPolicy());
		File dir = FileUtils.getDataRootDir();
		c.setInputFiles(new File(dir, "points.txt"));
		c.initialize();
		c.clustering();
		
		System.out.println("== Clustered points ==");
		ClusteringResult<Point2D> result = c.getClusteringResult();
		ClusteringUtils.print2DClusterPoints(result.getClusteredPoints());
		
		// print centroids
		System.out.println("== Centroid points ==");
		for(CenterPoint p : c.getCenterPointSet()) {
			System.out.println(p.getX() + "," + p.getY() + "," + p.getId());
		}
	}

}
