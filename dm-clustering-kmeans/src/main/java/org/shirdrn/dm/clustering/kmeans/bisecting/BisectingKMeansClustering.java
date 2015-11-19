package org.shirdrn.dm.clustering.kmeans.bisecting;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.dm.clustering.common.ClusterPoint;
import org.shirdrn.dm.clustering.common.Clustering2D;
import org.shirdrn.dm.clustering.common.ClusteringResult;
import org.shirdrn.dm.clustering.common.Point2D;
import org.shirdrn.dm.clustering.common.utils.ClusteringUtils;
import org.shirdrn.dm.clustering.common.utils.FileUtils;
import org.shirdrn.dm.clustering.common.utils.MetricUtils;
import org.shirdrn.dm.clustering.kmeans.KMeansClustering;
import org.shirdrn.dm.clustering.kmeans.common.Centroid;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Bisecting k-means clustering algorithm.
 *
 * @author yanjun
 */
public class BisectingKMeansClustering extends Clustering2D {

	private static final Log LOG = LogFactory.getLog(BisectingKMeansClustering.class);
	private final int k;
	private final Set<Centroid> centroidSet = Sets.newTreeSet(); 
	
	public BisectingKMeansClustering(int k) {
		super(1);
		this.k = k;
	}
	
	@Override
	public void clustering() {
		// parse sample files
		final List<Point2D> allPoints = Lists.newArrayList();
		FileUtils.read2DPointsFromFiles(allPoints, "[\t,;\\s]+", inputFiles);
		
		final int bisectingK = 2;
		int bisectingIterations = 0;
		float maxMovingPointRate = 0.01f;
		int maxInterations = 20;
		List<Point2D> points = allPoints;
		final Map<Centroid, Set<ClusterPoint<Point2D>>> clusteringPoints = Maps.newConcurrentMap();
		while(clusteringPoints.size() <= k) {
			LOG.info("Start bisecting iterations: #" + (++bisectingIterations) + ", bisectingK=" + bisectingK + ", maxMovingPointRate=" + maxMovingPointRate + 
					", maxInterations=" + maxInterations + ", parallism=" + parallism);
			// for k=tempK, execute k-means clustering
			final KMeansClustering kmeans = new KMeansClustering(bisectingK, maxMovingPointRate, maxInterations, parallism);
			kmeans.initialize(points);
			kmeans.clustering();
			// the clustering result should have 2 clusters
			ClusteringResult<Point2D> clusteringResult = kmeans.getClusteringResult();
			
			// merge cluster points for choosing cluster bisected again
			int id = generateNewClusterId(clusteringPoints.keySet());
			Set<Centroid> bisectedCentroids = kmeans.getCentroidSet();
			Map<Integer, Set<ClusterPoint<Point2D>>> bisectedClusterPoints = clusteringResult.getClusteredPoints();
			merge(clusteringPoints, id, bisectedCentroids, bisectedClusterPoints);
			
			if(clusteringPoints.size() == k) {
				break;
			}
			
			// compute cluster to be bisected
			ClusterInfo cluster = chooseClusterToBisect(clusteringPoints);
			LOG.info("Cluster to be bisected: " + cluster);
			
			points = Lists.newArrayList();
			for(ClusterPoint<Point2D> cp : cluster.clusterPointsToBisect) {
				points.add(cp.getPoint());
			}
			
			LOG.info("Finish bisecting iterations: #" + bisectingIterations + ", clusterSize=" + clusteringPoints.size());
		}
		
		// finally transform to result format
		Iterator<Entry<Centroid, Set<ClusterPoint<Point2D>>>> iter = clusteringPoints.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<Centroid, Set<ClusterPoint<Point2D>>> entry = iter.next();
			clusteredPoints.put(entry.getKey().getId(), entry.getValue());
			centroidSet.add(entry.getKey());
		}
	}

	private void merge(final Map<Centroid, Set<ClusterPoint<Point2D>>> clusteringPoints, 
			int id, Set<Centroid> bisectedCentroids, 
			Map<Integer, Set<ClusterPoint<Point2D>>> bisectedClusterPoints) {
		int startId = id;
		for(Centroid centroid : bisectedCentroids) {
			Set<ClusterPoint<Point2D>> set = bisectedClusterPoints.get(centroid.getId());
			centroid.setId(startId);
			// here, we don't update cluster id for ClusterPoint object in set,
			// we should do it until iterate the set for choosing cluster to be bisected
			clusteringPoints.put(centroid, set);
			startId++;
		}
	}

	private int generateNewClusterId(Set<Centroid> keptCentroids) {
		int id = -1;
		for(Centroid centroid : keptCentroids) {
			if(centroid.getId() > id) {
				id = centroid.getId();
			}
		}
		return id + 1;
	}

	private ClusterInfo chooseClusterToBisect(Map<Centroid, Set<ClusterPoint<Point2D>>> clusteringPoints) {
		double maxSSE = 0.0;
		int clusterIdWithMaxSSE = -1;
		Centroid centroidToBisect = null;
		Set<ClusterPoint<Point2D>> clusterToBisect = null;
		Iterator<Entry<Centroid, Set<ClusterPoint<Point2D>>>> iter = clusteringPoints.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<Centroid, Set<ClusterPoint<Point2D>>> entry = iter.next();
			Centroid centroid = entry.getKey();
			Set<ClusterPoint<Point2D>> cpSet = entry.getValue();
			double sse = computeSSE(centroid, cpSet);
			if(sse > maxSSE) {
				maxSSE = sse;
				clusterIdWithMaxSSE = centroid.getId();
				centroidToBisect = centroid;
				clusterToBisect = cpSet;
			}
		}
		// remove centroid from collected clusters map
		clusteringPoints.remove(centroidToBisect);
		return new ClusterInfo(clusterIdWithMaxSSE, clusterToBisect, maxSSE);
	}
	
	private double computeSSE(Centroid centroid, Set<ClusterPoint<Point2D>> cpSet) {
		double sse = 0.0;
		for(ClusterPoint<Point2D> cp : cpSet) {
			// update cluster id for ClusterPoint object
			cp.setClusterId(centroid.getId());
			double distance = MetricUtils.euclideanDistance(cp.getPoint(), centroid);
			sse += distance * distance;
		}
		return sse;
	}
	
	private class ClusterInfo {
		
		private final int id;
		private final Set<ClusterPoint<Point2D>> clusterPointsToBisect;
		private final double maxSSE;
		
		public ClusterInfo(int id, Set<ClusterPoint<Point2D>> clusterPointsToBisect, double maxSSE) {
			super();
			this.id = id;
			this.clusterPointsToBisect = clusterPointsToBisect;
			this.maxSSE = maxSSE;
		}
		
		@Override
		public String toString() {
			return "ClusterInfo[id=" + id + ", points=" + clusterPointsToBisect.size() + ". maxSSE=" + maxSSE + "]";
		}
	}
	
	public Set<Centroid> getCentroidSet() {
		return centroidSet;
	}
	
	public static void main(String[] args) {
		int k = 10;
		BisectingKMeansClustering bisecting = new BisectingKMeansClustering(k);
		File dir = FileUtils.getDataRootDir();
		bisecting.setInputFiles(new File(dir, "xy_zfmx.txt"));
		bisecting.clustering();
		
		System.out.println("== Clustered points ==");
		ClusteringResult<Point2D> result = bisecting.getClusteringResult();
		ClusteringUtils.print2DClusterPoints(result.getClusteredPoints());
		
		// print centroids
		System.out.println("== Centroid points ==");
		for(Centroid p : bisecting.getCentroidSet()) {
			System.out.println(p.getX() + "," + p.getY() + "," + p.getId());
		}
	}

}
