package org.shirdrn.dm.clustering.kmeans.kmedoids;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.dm.clustering.common.Clustering2D;
import org.shirdrn.dm.clustering.common.DistanceCache;
import org.shirdrn.dm.clustering.common.NamedThreadFactory;
import org.shirdrn.dm.clustering.common.Point2D;
import org.shirdrn.dm.clustering.common.utils.FileUtils;
import org.shirdrn.dm.clustering.common.utils.MetricUtils;
import org.shirdrn.dm.clustering.kmeans.RandomlySelectInitialCentroidsPolicy;
import org.shirdrn.dm.clustering.kmeans.common.Centroid;
import org.shirdrn.dm.clustering.kmeans.common.SelectInitialCentroidsPolicy;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Basic k-medoids clustering algorithm. 
 *
 * @author yanjun
 */
public class KMedoidsClustering extends Clustering2D {

	private static final Log LOG = LogFactory.getLog(KMedoidsClustering.class);
	private final int k;
	private final List<Point2D> allPoints = Lists.newArrayList();
	private TreeSet<Centroid> medoidSet;
	private final SelectInitialCentroidsPolicy selectInitialCentroidsPolicy;
	private final List<NearestMedoidSeeker> seekers = Lists.newArrayList();
	private int taskIndex = 0;
	private int seekerQueueSize = 200;
	private CountDownLatch latch;
	private final ExecutorService executorService;
	private volatile boolean completeToAssignTask = false;
	private final Random random = new Random();
	private final DistanceCache distanceCache;
	
	private final Object signal = new Object();
	
	public KMedoidsClustering(int k, int parallism) {
		super(parallism);
		Preconditions.checkArgument(k > 0, "Required: k > 0!");
		this.k = k;
		selectInitialCentroidsPolicy = new RandomlySelectInitialCentroidsPolicy();
		distanceCache = new DistanceCache(Integer.MAX_VALUE);
		executorService = Executors.newCachedThreadPool(new NamedThreadFactory("SEEKER"));
		latch = new CountDownLatch(parallism);
	}
	
	@Override
	public void clustering() {
		// parse sample files
		FileUtils.read2DPointsFromFiles(allPoints, "[\t,;\\s]+", inputFiles);
		LOG.info("Total points: count=" + allPoints.size());
		
		final TreeSet<Centroid> medoids = selectInitialCentroidsPolicy.select(k, allPoints);
		final Set<Point2D> centerPoints = Sets.newHashSet();
		for(Centroid c : medoids) {
			centerPoints.add(new Point2D(c.getX(), c.getY()));
		}
		LOG.info("Initial selected medoids: " + medoids);
		
		for (int i = 0; i < parallism; i++) {
			final NearestMedoidSeeker seeker = new NearestMedoidSeeker(seekerQueueSize, medoids);
			executorService.execute(seeker);
			seekers.add(seeker);
		}
		
		// assign task to seeker threads
		TreeMap<Centroid, List<Point2D>> medoidWithNearestPointSet = Maps.newTreeMap();
		try {
			for(Point2D p : allPoints) {
				if(!centerPoints.contains(p)) {
					selectSeeker().q.put(p);
				}
			}
		} catch(Exception e) {
			throw Throwables.propagate(e);
		} finally {
			try {
				completeToAssignTask = true;
				latch.await();
			} catch (InterruptedException e) { }
		}
		
		// merge result
		for(NearestMedoidSeeker seeker : seekers) {
			Iterator<Entry<Centroid, List<Point2D>>> iter = seeker.clusteringNearestPoints.entrySet().iterator();
			while(iter.hasNext()) {
				Entry<Centroid, List<Point2D>> entry = iter.next();
				List<Point2D> set = medoidWithNearestPointSet.get(entry.getKey());
				if(set == null) {
					set = Lists.newArrayList();
					medoidWithNearestPointSet.put(entry.getKey(), set);
				}
				set.addAll(entry.getValue());
			}
		}
		double previousSAD = computeSAD(medoidWithNearestPointSet);
		
		RandomPoint randomPoint = selectNonCenterPointRandomly(medoidWithNearestPointSet);
		double currentSAD = computeSAD(medoidWithNearestPointSet, randomPoint);
		if(currentSAD - previousSAD < 0) {
			// randomly selected point substitutes for medoid
			
		} else {
			// compute for next selected non-medoid point
			
		}
		
		LOG.info("Shutdown executor service: " + executorService);
		executorService.shutdown();
	}
	
	private double computeSAD(TreeMap<Centroid, List<Point2D>> medoidWithNearestPointSet) {
		return computeSAD(medoidWithNearestPointSet, null);
	}

	private double computeSAD(TreeMap<Centroid, List<Point2D>> medoidWithNearestPointSet, RandomPoint randomPoint) {
		double sad = 0.0; // sum of absolute differences
		for(Centroid medoid : medoidWithNearestPointSet.keySet()) {
			double distances = 0.0;
			List<Point2D> points = medoidWithNearestPointSet.get(medoid);
			if(randomPoint == null || !randomPoint.medoid.equals(medoid)) {
				for(Point2D p : points) {
					distances += distanceCache.computeDistance(medoid.toPoint(), p);
				}
			} else {
				if(randomPoint.medoid.equals(medoid)) {
					for (int i = 0; i < points.size(); i++) {
						if(randomPoint.pointIndex != i) {
							distances += distanceCache.computeDistance(randomPoint.medoid.toPoint(), points.get(i));
						} else {
							distances += distanceCache.computeDistance(randomPoint.medoid.toPoint(), medoid);
						}
					}
				}
			}
			sad += distances;
		}
		return sad;
	}
	
	private RandomPoint selectNonCenterPointRandomly(TreeMap<Centroid, List<Point2D>> medoidWithNearestPointSet) {
		Centroid medoid = Lists.newArrayList(medoidWithNearestPointSet.keySet()).get(random.nextInt(k));
		List<Point2D> pointList = medoidWithNearestPointSet.get(medoid);
		int index = random.nextInt(pointList.size());
		return new RandomPoint(medoid, pointList.get(index), index);
	}
	
	class RandomPoint {
		
		final Centroid medoid;
		final Point2D point;
		final int pointIndex;
		
		public RandomPoint(Centroid medoid, Point2D point, int pointIndex) {
			super();
			this.medoid = medoid;
			this.point = point;
			this.pointIndex = pointIndex;
		}
	}

	public TreeSet<Centroid> getMedoidSet() {
		return medoidSet;
	}
	
	private NearestMedoidSeeker selectSeeker() {
		int index = taskIndex++ % parallism;
		return seekers.get(index);
	}
	
	private class NearestMedoidSeeker implements Runnable {
		
		private final Log LOG = LogFactory.getLog(NearestMedoidSeeker.class);
		private final BlockingQueue<Point2D> q;
		private final TreeSet<Centroid> initialMedoids;
		private final  Map<Centroid, List<Point2D>> clusteringNearestPoints = Maps.newHashMap();
		
		public NearestMedoidSeeker(int qsize, TreeSet<Centroid> initialMedoids) {
			q = new LinkedBlockingQueue<Point2D>(qsize);
			this.initialMedoids = initialMedoids;
		}
		
		@Override
		public void run() {
			try {
				while(!completeToAssignTask) {
					while(!q.isEmpty()) {
						final Point2D p1 = q.poll();
						double minDistance = Double.MAX_VALUE;
						Centroid nearestMedoid = null;
						for(Centroid medoid : initialMedoids) {
							final Point2D p2 = medoid.toPoint();
							Double distance = distanceCache.computeDistance(p1, p2);
							if(distance < minDistance) {
								minDistance = distance;
								nearestMedoid = medoid;
							}
						}
						
						List<Point2D> points = clusteringNearestPoints.get(nearestMedoid);
						if(points == null) {
							points = Lists.newArrayList();
							clusteringNearestPoints.put(nearestMedoid, points);
						}
						points.add(p1);
					}
					Thread.sleep(150);
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				latch.countDown();
			}
			
		}
	}

}
