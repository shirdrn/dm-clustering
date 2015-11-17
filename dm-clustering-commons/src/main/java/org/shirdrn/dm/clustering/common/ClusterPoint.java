package org.shirdrn.dm.clustering.common;

public interface ClusterPoint<P> {

	int getClusterId();
	void setClusterId(int clusterId);
	P getPoint();
}
