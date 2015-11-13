package org.shirdrn.dm.clustering.common.utils;

import org.shirdrn.dm.clustering.common.Point2D;

public class MetricUtils {

	public static double euclideanDistance(Point2D p1, Point2D p2) {
		double sum = 0.0;
		double diffX = p1.getX() - p2.getX();
		double diffY = p1.getY() - p2.getY();
		sum += diffX * diffX + diffY * diffY;
		return Math.sqrt(sum);
	}
	
	public static double euclideanDistance(Point2D[] p1, Point2D[] p2) {
		double sum = 0.0;
		for (int i = 0; i < p1.length; i++) {
			double diffX = p1[i].getX() - p2[i].getX();
			double diffY = p1[i].getY() - p2[i].getY();
			sum += diffX * diffX + diffY * diffY;
		}
		return Math.sqrt(sum);
	}
	
}
