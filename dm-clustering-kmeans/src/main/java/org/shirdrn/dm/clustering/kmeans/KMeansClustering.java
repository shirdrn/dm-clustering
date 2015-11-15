package org.shirdrn.dm.clustering.kmeans;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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
	private float clusterChangedPointRate;
	private final List<Point2D> allPoints = Lists.newArrayList();
	private final SelectInitialCentroidsPolicy selectInitialCentroidsPolicy;
	private final ExecutorService executorService;
	private CountDownLatch latch;
	private final int parallism;
	private int taskIndex = 0;
	private int calculatorQueueSize = 200;
	private final List<CentroidCalculator> calculators = Lists.newArrayList();
	private volatile boolean completeToAssignTask = false;
	
	public KMeansClustering(int k, float clusterChangedPointRate, int parallism) {
		super();
		this.k = k;
		this.clusterChangedPointRate = clusterChangedPointRate;
		selectInitialCentroidsPolicy = new RandomlySelectInitialCentroidsPolicy();
		latch = new CountDownLatch(parallism);
		this.parallism = parallism;
		executorService = Executors.newCachedThreadPool(new NamedThreadFactory("CENTROID"));
	}
	
	@Override
	public void clustering() {
		// parse sample files
		FileUtils.read2DPointsFromFiles(allPoints, "[\t,;\\s]+", inputFiles);
		
		// start centroid calculators
		for (int i = 0; i < parallism; i++) {
			CentroidCalculator calculator = new CentroidCalculator(calculatorQueueSize);
			calculators.add(calculator);
			executorService.execute(calculator);
			LOG.info("Centroid calculator started: " + calculator);
		}
		
		Set<Point2D> centroids = selectInitialCentroidsPolicy.select(k, allPoints);
		int round = 0;
		CentroidSetWithClusteringPoints lastClusteringResult = null;
		int totalPointCount = allPoints.size();
		float currentClusterChangedPointRate = 1.0f;
		try {
			// enter clustering iteration procedure
			while(currentClusterChangedPointRate > clusterChangedPointRate) {
				LOG.info("Start round: #" + (++round));
				CentroidSetWithClusteringPoints currentClusteringResult = computeCentroids(centroids);
				// compute centroid convergence status
				int currentChangedPoints = 0;
				if(lastClusteringResult == null) {
					lastClusteringResult = currentClusteringResult;
					currentChangedPoints = totalPointCount;
					reset();
					continue;
				} else {
					// compare 2 round result for centroid computation
					currentChangedPoints = analyzeChangedPoints(lastClusteringResult.clusteringPoints, currentClusteringResult.clusteringPoints);
				}
				reset();
				centroids = currentClusteringResult.centroids;
				currentClusterChangedPointRate = (float) currentChangedPoints / totalPointCount;
				LOG.info("Finish round: #" + (round));
			}
		} finally {
			LOG.info("Shutdown executor service: " + executorService);
			executorService.shutdown();
		}
	}

	private int analyzeChangedPoints(Map<Point2D, Multiset<Point2D>> lastClusteringPoints,
			Map<Point2D, Multiset<Point2D>> currentClusteringPoints) {
		// Map<current, Map<last, intersected point count>>
		Map<Multiset<Point2D>, Map<Multiset<Point2D>, Integer>> intersectedClusterPointCounts = Maps.newHashMap();
		for(Multiset<Point2D> last : lastClusteringPoints.values()) {
			for(Multiset<Point2D> current : currentClusteringPoints.values()) {
				Map<Multiset<Point2D>, Integer> counts = intersectedClusterPointCounts.get(current);
				if(counts == null) {
					counts = Maps.newHashMap();
					intersectedClusterPointCounts.put(current, counts);
				}
				counts.put(last, Multisets.intersection(last, current).size());
			}
		}
		
		// compute changed points
		// TODO greedy policy, and order dependent
		Iterator<Entry<Multiset<Point2D>, Map<Multiset<Point2D>, Integer>>>  iter = intersectedClusterPointCounts.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<Multiset<Point2D>, Map<Multiset<Point2D>, Integer>> entry = iter.next();
			Multiset<Point2D> current = entry.getKey();
			Map<Multiset<Point2D>, Integer> countMap = entry.getValue();
			// sort map by DESC
			Entry[] a = new Entry[countMap.size()];
			a = countMap.entrySet().toArray(a);
			Arrays.sort(a, new Comparator<Entry>() {

				@Override
				public int compare(Entry o1, Entry o2) {
					Entry<Multiset<Point2D>, Integer> entry1 = (Entry<Multiset<Point2D>, Integer>) o1;
					Entry<Multiset<Point2D>, Integer> entry2 = (Entry<Multiset<Point2D>, Integer>) o2;
					return entry2.getValue() - entry1.getValue();
				}
				
			});
			
		}
		return 0;
	}

	private CentroidSetWithClusteringPoints computeCentroids(Set<Point2D> centroids) {
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
		Map<Point2D, Multiset<Point2D>> clusteringPoints = Maps.newHashMap();
		for(CentroidCalculator calculator : calculators) {
			for(Point2D centroid : calculator.localClusteredPoints.keySet()) {
				Multiset<Point2D> globalPoints = clusteringPoints.get(centroid);
				if(globalPoints == null) {
					globalPoints = HashMultiset.create();
					clusteringPoints.put(centroid, globalPoints);
				}
				globalPoints.addAll(calculator.localClusteredPoints.get(centroid));
			}
			calculator.reset();
		}
		
		// re-compute centroids
		Set<Point2D> newCentroids = Sets.newHashSet();
		Iterator<Entry<Point2D, Multiset<Point2D>>> iter = clusteringPoints.entrySet().iterator();
		while(iter.hasNext()) {
			newCentroids.add(MetricUtils.meanCentroid(iter.next().getValue()));
		}
		return new CentroidSetWithClusteringPoints(newCentroids, clusteringPoints);
	}
	
	private class CentroidSetWithClusteringPoints {
		
		private final Set<Point2D> centroids;
		private final Map<Point2D, Multiset<Point2D>> clusteringPoints;
		
		public CentroidSetWithClusteringPoints(Set<Point2D> centroids, Map<Point2D, Multiset<Point2D>> clusteringPoints) {
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
		// Map<centroid, points belonging to this centroid>
		private Map<Point2D, Multiset<Point2D>> localClusteredPoints = Maps.newHashMap();
		
		public CentroidCalculator(int qsize) {
			q = new LinkedBlockingQueue<Task>(qsize);
		}
		
		@Override
		public void run() {
			Map<Point2D, TreeSet<Distance>> distances = Maps.newHashMap();
			try {
				while(!completeToAssignTask) {
					while(!q.isEmpty()) {
						try {
							Task task = q.take();
							Point2D p1 = task.point;
							TreeSet<Distance> distanceSet = distances.get(task.point);
							if(distanceSet == null) {
								// use default ASC sort order
								distanceSet = Sets.newTreeSet();
								distances.put(p1, distanceSet);
							}
							for(Point2D centroid : task.centroids) {
								distanceSet.add(new Distance(p1, centroid, MetricUtils.euclideanDistance(p1, centroid)));
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			} finally {
				// assign points to a nearest centroid
				for(Point2D p : distances.keySet()) {
					Point2D centroidBelongedTo = distances.get(p).iterator().next().centroid;
					Multiset<Point2D> pointsBelongingToCentroid = localClusteredPoints.get(centroidBelongedTo);
					if(pointsBelongingToCentroid == null) {
						pointsBelongingToCentroid = HashMultiset.create();
					}
					pointsBelongingToCentroid.add(centroidBelongedTo);
				}
				LOG.info("Calculator finished: round=" + (++round));
				latch.countDown();
			}
			
		}
		
		public void reset() {
			localClusteredPoints = null;
			localClusteredPoints = Maps.newHashMap();
		}
	}
	
	private class Task {
		
		protected final Point2D point;
		protected final Set<Point2D> centroids;
		
		public Task(Point2D point, Set<Point2D> centroids) {
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
		
		private final Point2D point;
		private final Point2D centroid;
		private double distance = 0.0;
		
		public Distance(Point2D point, Point2D centroid) {
			this.point = point;
			this.centroid = centroid;
		}
		
		public Distance(Point2D point, Point2D centroid, double distance) {
			this(point, centroid);
			this.distance = distance;
		}

		@Override
		public int compareTo(Distance o) {
			double diff = this.distance - o.distance;
			return diff<0 ? -1 : (diff>0 ? 1 : 0);
		}
	}

}
