package org.shirdrn.dm.clustering.kmeans.bisecting;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.shirdrn.dm.clustering.common.ClusterPoint;
import org.shirdrn.dm.clustering.common.Clustering2D;
import org.shirdrn.dm.clustering.common.ClusteringResult;
import org.shirdrn.dm.clustering.common.Point2D;
import org.shirdrn.dm.clustering.common.utils.FileUtils;
import org.shirdrn.dm.clustering.common.utils.MetricUtils;
import org.shirdrn.dm.clustering.kmeans.KMeansClustering;
import org.shirdrn.dm.clustering.kmeans.common.Centroid;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Bisecting k-means clustering algorithm.
 *
 * @author yanjun
 */
public class BisectingKMeansClustering extends Clustering2D {

	private final int k;
	
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
		List<Point2D> points = allPoints;
		Set<ClusterPoint<Point2D>> clusteringPoints = Sets.newHashSet();
		while(tempK < k) {
			// for k=tempK, execute k-means clustering
			KMeansClustering kmeansClustering = new KMeansClustering(tempK, maxMovingPointRate, maxInterations, parallism);
			kmeansClustering.initialize(points);
			kmeansClustering.clustering();
			ClusteringResult<Point2D> clusteringResult = kmeansClustering.getClusteringResult();
			TreeSet<Centroid> centroids = rearrange(kmeansClustering.getCentroidSet(), clusteringResult.getClusteredPoints());
			
			// compute cluster to be bisected
			ClusterInfo cluster = chooseClusterToBisect(centroids, clusteringResult.getClusteredPoints());
			Set<ClusterPoint<Point2D>> bisectingClusterPoints = clusteredPoints.remove(cluster.id);
			
		}
	}

	private TreeSet<Centroid> rearrange(Set<Centroid> centroidSet, Map<Integer, Set<ClusterPoint<Point2D>>> clusteringPoints) {
		TreeSet<Centroid> centroids = Sets.newTreeSet();
		
		return null;
	}

	private ClusterInfo chooseClusterToBisect(Set<Centroid> centroids, Map<Integer, Set<ClusterPoint<Point2D>>> kmeansClusteredPoints) {
		double maxSSE = 0.0;
		int clusterIdWithMaxSSE = -1;
		Iterator<Entry<Integer, Set<ClusterPoint<Point2D>>>> iter = kmeansClusteredPoints.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<Integer, Set<ClusterPoint<Point2D>>> entry = iter.next();
			int clusterId = entry.getKey();
			Set<ClusterPoint<Point2D>> set = entry.getValue();
			double sse = computeSSE(getCentroid(centroids, clusterId), set);
			if(sse > maxSSE) {
				maxSSE = sse;
				clusterIdWithMaxSSE = clusterId;
			}
		}
		return new ClusterInfo(clusterIdWithMaxSSE, maxSSE);
	}
	
	private double computeSSE(Centroid centroid, Set<ClusterPoint<Point2D>> points) {
		double sse = 0.0;
		for(ClusterPoint<Point2D> p : points) {
			double distance = MetricUtils.euclideanDistance(p.getPoint(), centroid);
			sse += distance * distance;
		}
		return sse;
	}
	
	private Centroid getCentroid(Set<Centroid> centroids, int id) {
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
		private final double maxSSE;
		
		public ClusterInfo(int id, double maxSSE) {
			super();
			this.id = id;
			this.maxSSE = maxSSE;
		}
	}

}
