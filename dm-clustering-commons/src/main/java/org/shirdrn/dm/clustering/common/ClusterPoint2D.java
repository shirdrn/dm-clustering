package org.shirdrn.dm.clustering.common;


public class ClusterPoint2D implements ClusterPoint<Point2D> {

	private int clusterId;
	private final Point2D point;
	
	public ClusterPoint2D(Double x, Double y) {
		this.point = new Point2D(x, y);
	}
	
	public ClusterPoint2D(Double x, Double y, int clusterId) {
		this(x, y);
		this.clusterId = clusterId;
	}
	
	public ClusterPoint2D(Point2D point, int clusterId) {
		this(point.getX(), point.getY());
		this.clusterId = clusterId;
	}
	
	@Override
	public Point2D getPoint() {
		return point;
	}

	@Override
	public int getClusterId() {
		return clusterId;
	}

	@Override
	public void setClusterId(int clusterId) {
		this.clusterId = clusterId;
	}

}
