package org.shirdrn.dm.clustering.kmeans;

import java.io.File;
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
import org.shirdrn.dm.clustering.common.AbstractClustering;
import org.shirdrn.dm.clustering.common.NamedThreadFactory;
import org.shirdrn.dm.clustering.common.Point2D;
import org.shirdrn.dm.clustering.common.utils.FileUtils;
import org.shirdrn.dm.clustering.common.utils.MetricUtils;
import org.shirdrn.dm.clustering.kmeans.common.Centroid;
import org.shirdrn.dm.clustering.kmeans.common.SelectInitialCentroidsPolicy;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import com.google.common.collect.Sets;

public class KMeansClustering extends AbstractClustering {

	private static final Log LOG = LogFactory.getLog(KMeansClustering.class);
	private int k;
	private float maxMovingPointRate;
	private final List<Point2D> allPoints = Lists.newArrayList();
	private final SelectInitialCentroidsPolicy selectInitialCentroidsPolicy;
	private final ExecutorService executorService;
	private CountDownLatch latch;
	private final int parallism;
	private int taskIndex = 0;
	private int calculatorQueueSize = 200;
	private final List<CentroidCalculator> calculators = Lists.newArrayList();
	private volatile boolean completeToAssignTask = false;
	private volatile boolean clusteringCompletedFinally = false;
	private final Object controllingSignal = new Object();
	
	public KMeansClustering(int k, float maxMovingPointRate, int parallism) {
		super();
		this.k = k;
		this.maxMovingPointRate = maxMovingPointRate;
		selectInitialCentroidsPolicy = new RandomlySelectInitialCentroidsPolicy();
		latch = new CountDownLatch(parallism);
		this.parallism = parallism;
		executorService = Executors.newCachedThreadPool(new NamedThreadFactory("CENTROID"));
		LOG.info("Init: k=" + k + ", maxMovingPointRate=" + maxMovingPointRate + ", parallism=" + parallism + 
				", selectInitialCentroidsPolicy=" + selectInitialCentroidsPolicy.getClass().getName());
	}
	
	@Override
	public void clustering() {
		// parse sample files
		FileUtils.read2DPointsFromFiles(allPoints, "[\t,;\\s]+", inputFiles);
		LOG.info("Total points: count=" + allPoints.size());
		
		// start centroid calculators
		for (int i = 0; i < parallism; i++) {
			CentroidCalculator calculator = new CentroidCalculator(calculatorQueueSize);
			calculators.add(calculator);
			executorService.execute(calculator);
			LOG.info("Centroid calculator started: " + calculator);
		}
		
		// sort by centroid id ASC
		TreeSet<Centroid> centroids = selectInitialCentroidsPolicy.select(k, allPoints);
		LOG.info("Initial selected centroids: " + centroids);
		
		int round = 0;
		CentroidSetWithClusteringPoints lastClusteringResult = null;
		CentroidSetWithClusteringPoints currentClusteringResult = null;
		int totalPointCount = allPoints.size();
		float currentClusterMovingPointRate = 1.0f;
		try {
			// enter clustering iteration procedure
			while(currentClusterMovingPointRate > maxMovingPointRate) {
				LOG.info("Start round: #" + (++round));
				// signal calculators
				notifyAllCalculators();
				LOG.info("Notify all calculator to process tasks...");
				
				currentClusteringResult = computeCentroids(centroids);
				LOG.info("Recomputed centroids: " + centroids);
				
				// compute centroid convergence status
				int numMovingPoints = 0;
				if(lastClusteringResult == null) {
					lastClusteringResult = currentClusteringResult;
					numMovingPoints = totalPointCount;
				} else {
					// compare 2 round result for centroid computation
					numMovingPoints = analyzeMovingPoints(lastClusteringResult.clusteringPoints, currentClusteringResult.clusteringPoints);
				}
				centroids = currentClusteringResult.centroids;
				currentClusterMovingPointRate = (float) numMovingPoints / totalPointCount;
				LOG.info("Clustering meta: k=" + k + 
						", numMovingPoints=" + numMovingPoints + 
						", totalPointCount=" + totalPointCount +
						", currentClusterMovingPointRate=" + currentClusterMovingPointRate );
				
				// reset some structures
				reset();
				for(CentroidCalculator calculator : calculators) {
					calculator.reset();
				}
				
				LOG.info("Finish round: #" + round);
			}
		} finally {
			// notify all calculators to exit normally
			clusteringCompletedFinally = true;
			LOG.info("Notify all calculators to exit normally...");
			notifyAllCalculators();
			
			LOG.info("Shutdown executor service: " + executorService);
			executorService.shutdown();
			
			// process final clustering result
			LOG.info("Final clustering result: ");
			Iterator<Entry<Centroid, Multiset<Point2D>>> iter = currentClusteringResult.clusteringPoints.entrySet().iterator();
			int seqNo = 0;
			while(iter.hasNext()) {
				Entry<Centroid, Multiset<Point2D>> entry = iter.next();
				System.out.println(seqNo + ". [" + entry.getKey() + "] " + entry.getValue().size());
			}
		}
		
		// output clustering result
		outputResult(currentClusteringResult.clusteringPoints);
	}

	private void outputResult(TreeMap<Centroid, Multiset<Point2D>> clusterPoints) {
		Iterator<Entry<Centroid, Multiset<Point2D>>> iter = clusterPoints.entrySet().iterator();
		System.out.println(" == Cluster centroids == ");
		while(iter.hasNext()) {
			Entry<Centroid, Multiset<Point2D>> entry = iter.next();
			Centroid c = entry.getKey();
			System.out.println(c.getX() + "," + c.getY() + "," + c.getId());
		}
		
		System.out.println(" == Cluster points == ");
		iter = clusterPoints.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<Centroid, Multiset<Point2D>> entry = iter.next();
			for(Point2D p : entry.getValue()) {
				System.out.println(p.getX() + "," + p.getY() + "," + entry.getKey().getId());
			}
		}
	}

	private void notifyAllCalculators() {
		synchronized(controllingSignal) {
			controllingSignal.notifyAll();
		}
	}

	private int analyzeMovingPoints(TreeMap<Centroid, Multiset<Point2D>> lastClusteringPoints,
			TreeMap<Centroid, Multiset<Point2D>> currentClusteringPoints) {
		// Map<current, Map<last, intersected point count>>
		Set<Point2D> movingPoints = Sets.newHashSet();
		Iterator<Entry<Centroid, Multiset<Point2D>>> lastIter = lastClusteringPoints.entrySet().iterator();
		Iterator<Entry<Centroid, Multiset<Point2D>>> currentIter = currentClusteringPoints.entrySet().iterator();
		while(lastIter.hasNext() && currentIter.hasNext()) {
			Entry<Centroid, Multiset<Point2D>> last = lastIter.next();
			Entry<Centroid, Multiset<Point2D>> current = currentIter.next();
			Multiset<Point2D> intersection = Multisets.intersection(last.getValue(), current.getValue());
			movingPoints.addAll(Multisets.difference(last.getValue(), intersection));
			movingPoints.addAll(Multisets.difference(current.getValue(), intersection));
		}
		return movingPoints.size();
	}

	private CentroidSetWithClusteringPoints computeCentroids(Set<Centroid> centroids) {
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
		TreeMap<Centroid, Multiset<Point2D>> clusteringPoints = Maps.newTreeMap();
		for(CentroidCalculator calculator : calculators) {
			for(Centroid centroid : calculator.localClusteredPoints.keySet()) {
				Multiset<Point2D> globalPoints = clusteringPoints.get(centroid);
				if(globalPoints == null) {
					globalPoints = HashMultiset.create();
					clusteringPoints.put(centroid, globalPoints);
				}
				globalPoints.addAll(calculator.localClusteredPoints.get(centroid));
			}
		}
		
		// re-compute centroids
		TreeSet<Centroid> newCentroids = Sets.newTreeSet();
		Iterator<Entry<Centroid, Multiset<Point2D>>> iter = clusteringPoints.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<Centroid, Multiset<Point2D>> entry = iter.next();
			Point2D point = MetricUtils.meanCentroid(entry.getValue());
			newCentroids.add(new Centroid(entry.getKey().getId(), point));
		}
		return new CentroidSetWithClusteringPoints(newCentroids, clusteringPoints);
	}
	
	private class CentroidSetWithClusteringPoints {
		
		private final TreeSet<Centroid> centroids;
		private final TreeMap<Centroid, Multiset<Point2D>> clusteringPoints;
		
		public CentroidSetWithClusteringPoints(TreeSet<Centroid> centroids, TreeMap<Centroid, Multiset<Point2D>> clusteringPoints) {
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
		private int round = 0;
		private final BlockingQueue<Task> q;
		// TreeMap<centroid, points belonging to this centroid>
		private TreeMap<Centroid, Multiset<Point2D>> localClusteredPoints = Maps.newTreeMap();
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
					synchronized(controllingSignal) {
						controllingSignal.wait();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			LOG.info("Received exit signal, exited. ");
		}

		private void process() {
			try {
				while(!completeToAssignTask) {
					while(!q.isEmpty()) {
						try {
							processedTasks++;
							Task task = q.poll();
							Point2D p1 = task.point;
							
							// assign points to a nearest centroid
							Distance minDistance = null;
							for(Centroid centroid : task.centroids) {
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
				}
			} finally {
				accumulatedProcessedTasks += processedTasks;
				latch.countDown();
				LOG.info("Calculator finished: round=" + (++round) + ", processedTasks=" + processedTasks + 
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
		protected final Set<Centroid> centroids;
		
		public Task(Point2D point, Set<Centroid> centroids) {
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
		private final Centroid centroid;
		private double distance = 0.0;
		
		public Distance(Point2D point, Centroid centroid) {
			this.point = point;
			this.centroid = centroid;
		}
		
		public Distance(Point2D point, Centroid centroid, double distance) {
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
		int k = 3;
		float maxMovingPointRate = 0.15f;
		int parallism = 5;
		KMeansClustering c = new KMeansClustering(k, maxMovingPointRate, parallism);
		File dir = FileUtils.getDataRootDir();
		c.setInputFiles(new File(dir, "xy_zfmx.txt"));
		c.clustering();
	}

}
