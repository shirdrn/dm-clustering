package org.shirdrn.dm.clustering.common;

public class MetricUtils {

	public static double euclideanDistance(Point2D p1, Point2D p2) {
		double sum = 0.0;
		double diffX = p1.x - p2.x;
		double diffY = p1.y - p2.y;
		sum += diffX * diffX + diffY * diffY;
		return Math.sqrt(sum);
	}
	
	public static double euclideanDistance(Point2D[] p1, Point2D[] p2) {
		double sum = 0.0;
		for (int i = 0; i < p1.length; i++) {
			double diffX = p1[i].x - p2[i].x;
			double diffY = p1[i].y - p2[i].y;
			sum += diffX * diffX + diffY * diffY;
		}
		return Math.sqrt(sum);
	}
	
}
