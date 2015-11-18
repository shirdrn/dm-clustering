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

/**
 * Bisecting k-means clustering algorithm.
 *
 * @author yanjun
 */
public class BisectingKMeansClustering extends Clustering2D {

	private static final Log LOG = LogFactory.getLog(BisectingKMeansClustering.class);
	private final int k;
	private Set<Centroid> centroidSet; 
	
	public BisectingKMeansClustering(int k) {
		super();
		this.k = k;
	}
	
	@Override
	public void clustering() {
		// parse sample files
		final List<Point2D> allPoints = Lists.newArrayList();
		FileUtils.read2DPointsFromFiles(allPoints, "[\t,;\\s]+", inputFiles);
		
		int tempK = 2;
		float maxMovingPointRate = 0.01f;
		int maxInterations = 20;
		int parallism = 4;
		KMeansClustering kmeansClustering = null;
		List<Point2D> points = allPoints;
		List<Set<ClusterPoint<Point2D>>> clusteringPoints = Lists.newArrayList();
		while(true) {
			LOG.info("Iteration: tempK=" + tempK + ", maxMovingPointRate=" + maxMovingPointRate + 
					", maxInterations=" + maxInterations + ", parallism=" + parallism);
			// for k=tempK, execute k-means clustering
			final KMeansClustering kmeans = new KMeansClustering(tempK, maxMovingPointRate, maxInterations, parallism);
			kmeansClustering = kmeans;
			kmeans.initialize(points);
			kmeans.clustering();
			ClusteringResult<Point2D> clusteringResult = kmeans.getClusteringResult();
			
			// merge cluster points for choosing cluster bisected again
			int id = generateNewClusterId(clusteringResult.getClusteredPoints().keySet());
			for(Set<ClusterPoint<Point2D>> set : clusteringPoints) {
				clusteringResult.getClusteredPoints().put(id, set);
				id++;
			}
			
			if(clusteringResult.getClusteredPoints().size() > k) {
				break;
			}
			
			// compute cluster to be bisected
			ClusterInfo cluster = chooseClusterToBisect(kmeans, clusteringResult);
			LOG.info("Chosen bisecting cluster: " + cluster);
			
			// collect clusters without being bisected
			Iterator<Entry<Integer, Set<ClusterPoint<Point2D>>>> iter = clusteringResult.getClusteredPoints().entrySet().iterator();
			while(iter.hasNext()) {
				Entry<Integer, Set<ClusterPoint<Point2D>>> entry = iter.next();
				int clusterId = entry.getKey();
				if(clusterId != cluster.id) {
					clusteringPoints.add(entry.getValue());
				}
			}
			
			points = Lists.newArrayList();
			for(ClusterPoint<Point2D> cp : cluster.clusterPoints) {
				points.add(cp.getPoint());
			}
		}
		
		clusteredPoints.putAll(kmeansClustering.getClusteringResult().getClusteredPoints());
		centroidSet = kmeansClustering.getCentroidSet();
	}

	private int generateNewClusterId(Set<Integer> existedClusterIds) {
		int id = -1;
		for(int i : existedClusterIds) {
			if(i > id) {
				id = i;
			}
		}
		return id + 1;
	}

	private ClusterInfo chooseClusterToBisect(KMeansClustering kmeansClustering, ClusteringResult<Point2D> clusteringResult) {
		Map<Integer, Set<ClusterPoint<Point2D>>> clusterPointMap = clusteringResult.getClusteredPoints();
		double maxSSE = 0.0;
		int clusterIdWithMaxSSE = -1;
		Set<ClusterPoint<Point2D>> clusterPoints = null;
		Iterator<Entry<Integer, Set<ClusterPoint<Point2D>>>> iter = clusterPointMap.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<Integer, Set<ClusterPoint<Point2D>>> entry = iter.next();
			int clusterId = entry.getKey();
			Set<ClusterPoint<Point2D>> set = entry.getValue();
			double sse = computeSSE(clusterId, set, kmeansClustering.getCentroidSet());
			if(sse > maxSSE) {
				maxSSE = sse;
				clusterIdWithMaxSSE = clusterId;
				clusterPoints = set;
			}
		}
		return new ClusterInfo(clusterIdWithMaxSSE, clusterPoints, maxSSE);
	}
	
	private double computeSSE(int clusterId, Set<ClusterPoint<Point2D>> points, Set<Centroid> centroids) {
		double sse = 0.0;
		for(ClusterPoint<Point2D> cp : points) {
			Centroid c = retrieveCentroid(centroids, clusterId);
			double distance = MetricUtils.euclideanDistance(cp.getPoint(), c);
			sse += distance * distance;
		}
		return sse;
	}
	
	private Centroid retrieveCentroid(Set<Centroid> centroids, int id) {
		Centroid centroid = null;
		for(Centroid c : centroids) {
			if(c.getId().intValue() == id) {
				centroid = c;
				break;
			}
		}
		return centroid;
	}
	
	private class ClusterInfo {
		
		private final int id;
		private final Set<ClusterPoint<Point2D>> clusterPoints;
		private final double maxSSE;
		
		public ClusterInfo(int id, Set<ClusterPoint<Point2D>> clusterPoints, double maxSSE) {
			super();
			this.id = id;
			this.clusterPoints = clusterPoints;
			this.maxSSE = maxSSE;
		}
		
		@Override
		public String toString() {
			return "ClusterInfo[id=" + id + ", points=" + clusterPoints.size() + ". maxSSE=" + maxSSE + "]";
		}
	}
	
	public Set<Centroid> getCentroidSet() {
		return centroidSet;
	}
	
	public static void main(String[] args) {
		int k = 10;
		BisectingKMeansClustering bisecting = new BisectingKMeansClustering(k);
		bisecting.clustering();
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
