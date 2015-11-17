package org.shirdrn.dm.clustering.common.utils;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.shirdrn.dm.clustering.common.ClusterPoint;
import org.shirdrn.dm.clustering.common.Point2D;

public class ClusteringUtils {

	public static void print2DClusterPoints(Map<Integer, Set<ClusterPoint<Point2D>>> clusterPoints) {
		Iterator<Entry<Integer, Set<ClusterPoint<Point2D>>>> iter = clusterPoints.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<Integer, Set<ClusterPoint<Point2D>>> entry = iter.next();
			int clusterId = entry.getKey();
			for(ClusterPoint<Point2D> cp : entry.getValue()) {
				System.out.println(cp.getPoint().getX() + "," + cp.getPoint().getY() + "," + clusterId);
			}
		}
	}
}
