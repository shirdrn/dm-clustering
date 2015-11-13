package org.shirdrn.dm.clustering.common;


public class ClusterPoint2D extends Point2D {

	private int clusterId;
	
	public ClusterPoint2D(Double x, Double y) {
		super(x, y);
	}
	
	public ClusterPoint2D(Double x, Double y, int clusterId) {
		super(x, y);
		this.clusterId = clusterId;
	}
	
	public ClusterPoint2D(Point2D point, int clusterId) {
		super(point.getX(), point.getY());
		this.clusterId = clusterId;
	}

	public int getClusterId() {
		return clusterId;
	}

	public void setClusterId(int clusterId) {
		this.clusterId = clusterId;
	}

}
