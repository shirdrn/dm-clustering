package org.shirdrn.dm.clustering.common;

import java.io.File;

public abstract class AbstractClustering<P> implements Clustering<P> {

	protected File[] inputFiles;
	protected final int parallism;
	protected final ClusteringResult<P> clusteringResult;
	
	public AbstractClustering() {
		this(1);
	}
	
	public AbstractClustering(int parallism) {
		super();
		this.parallism = parallism;
		this.clusteringResult = new GenericClusteringResult<P>();
	}
	
	@Override
	public int getClusteredCount() {
		return clusteringResult.getClusteredPoints().size();
	}
	
	@Override
	public void setInputFiles(File... inputFiles) {
		this.inputFiles = inputFiles;		
	}
	
	@Override
	public ClusteringResult<P> getClusteringResult() {
		return clusteringResult;
	}

}
