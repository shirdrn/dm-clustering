package org.shirdrn.dm.clustering.kmeans.utils;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.dm.clustering.common.CenterPoint;
import org.shirdrn.dm.clustering.common.DistanceCache;
import org.shirdrn.dm.clustering.common.Point2D;
import org.shirdrn.dm.clustering.kmeans.common.InitialCenterPointsSelectionPolicy;

import com.google.common.collect.Sets;

public class KMeansPlusPlusInitialCenterPointsSelectionPolicy implements InitialCenterPointsSelectionPolicy {

	private static final Log LOG = LogFactory.getLog(KMeansPlusPlusInitialCenterPointsSelectionPolicy.class);
	private final DistanceCache distanceCache;
	private final Random random = new Random();
	
	public KMeansPlusPlusInitialCenterPointsSelectionPolicy() {
		super();
		this.distanceCache = new DistanceCache(Integer.MAX_VALUE);
	}
	
	@Override
	public TreeSet<CenterPoint> select(int k, List<Point2D> points) {
		final Set<Point2D> centers =Sets.newHashSet();
		// select first center randomly
		Point2D firstCenter = points.get(random.nextInt(points.size()));
		centers.add(firstCenter);
		LOG.info("First center point got: " + firstCenter);
		
		while(centers.size() < k) {
			double maxProbability = 0.0;
			Point2D pointWithMaxProbability = null;
			for(Point2D p : points) {
				// compute point with minimum D2
				if(!centers.contains(p)) {
					double minD2 = Double.MAX_VALUE;
					for(Point2D center : centers) {
						double d = distanceCache.computeDistance(p, center);
						double d2 = d * d;
						if(minD2 > d2) {
							minD2 = d2;
						}
					}
					// compute point with max probability value
					if(maxProbability < minD2) {
						maxProbability = minD2;
						pointWithMaxProbability = p;
					}
				}
			}
			LOG.info("Center point got: " + pointWithMaxProbability);
			
			centers.add(pointWithMaxProbability);
		}
		
		TreeSet<CenterPoint> centerPoints = Sets.newTreeSet();
		int id = 0;
		for(Point2D center : centers) {
			centerPoints.add(new CenterPoint(id++, center));
		}
		return centerPoints;
	}

}
