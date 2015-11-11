package org.shirdrn.dm.clustering.dbscan;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.dm.clustering.common.MetricUtils;
import org.shirdrn.dm.clustering.common.NamedThreadFactory;
import org.shirdrn.dm.clustering.common.Point2D;

import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class EpsEstimator {

	private static final Log LOG = LogFactory.getLog(EpsEstimator.class);
	private final List<Point2D> allPoints = Lists.newArrayList();
	private Cache<Set<Point2D>, Double> distanceCache;
	private int k = 4;
	private int parallism = 5;
	private CountDownLatch latch;
	private final List<KDistanceCalculator> calculators = Lists.newArrayList();
	private final ExecutorService executorService;
	private int taskIndex = 0;
	private int calculatorCount = 5;
	private int calculatorQueueSize = 200;
	private volatile boolean completeToAssignTask = false;
	private double selectedKDistance;
	private EpsComputationPolicy epsComputationpsPolicy = new DefaultEpsComputationPolicy();
	
	public EpsEstimator() {
		this(4, 5);
	}
	
	public EpsEstimator(int k, int parallism) {
		super();
		this.k = k;
		this.parallism = parallism;
		distanceCache = CacheBuilder.newBuilder().maximumSize(Integer.MAX_VALUE).build();
		latch = new CountDownLatch(parallism);
		executorService = Executors.newCachedThreadPool(new NamedThreadFactory("KDCALC"));
		LOG.info("Config: k=" + k + ", parallism=" + parallism);
	}
	
	public Iterator<Point2D> allPointIterator() {
		return allPoints.iterator();
	}
	
	public EpsEstimator computeKDistance(File... files) {
		// parse sample files
		try {
			BufferedReader reader = null;
			for(File file : files) {
				try {
					reader = new BufferedReader(new FileReader(file.getAbsoluteFile()));
					String point = null;
					while((point = reader.readLine()) != null) {
						String[] a = point.split("[\t,;\\s]+");
						if(a.length == 2) {
							KPoint2D kp = new KPoint2D(Double.parseDouble(a[0]), Double.parseDouble(a[1]));
							if(!allPoints.contains(kp)) {
								allPoints.add(kp);
							}
						}
					}
				} catch (Exception e1) {
					e1.printStackTrace();
				} finally {
					if(reader != null) {
						reader.close();
					}
				}
			}
			
			// compute k-distance
			try {
				for (int i = 0; i < parallism; i++) {
					KDistanceCalculator calculator = new KDistanceCalculator(calculatorQueueSize);
					calculators.add(calculator);
					executorService.execute(calculator);
					LOG.info("k-distance calculator started: " + calculator);
				}
				calculatorCount = calculators.size();
				
				// assign point tasks
				for(int i=0; i<allPoints.size(); i++) {
					while(true) {
						KDistanceCalculator calculator = getCalculator();
						Task task = new Task(allPoints.get(i), i);
						boolean offered = calculator.q.offer(task);
						if(!offered) {
							continue;
						}
						LOG.debug("Assign Point[" + task.p + "] to " + calculator);
						break;
					}
				}
				LOG.info("Input: totalPoints=" + allPoints.size());
				
				completeToAssignTask = true;
			} finally {
				try {
					latch.await();
				} catch (InterruptedException e) { }
				LOG.info("Shutdown executor service: " + executorService);
				executorService.shutdown();
			}
		}  catch (Exception e) {
			throw Throwables.propagate(e);
		}
		return this;
	}
	
	public EpsEstimator estimateEps() {
		// sort k-distance s
		Collections.sort(allPoints, new Comparator<Point2D>() {

			@Override
			public int compare(Point2D o1, Point2D o2) {
				KPoint2D kp1 = (KPoint2D) o1;
				KPoint2D kp2 = (KPoint2D) o2;
				double diff = kp1.kDistance.doubleValue() - kp2.kDistance.doubleValue();
				if(diff == 0.0) {
					return 0;
				}
				return diff < 0 ? -1 : 1;
			}
			
		});

		LOG.info("Compute Eps...");
		selectedKDistance = 0.0045454545; // epsComputationpsPolicy.compute();
		LOG.info("Eps computed: selectedKDistance=" + selectedKDistance);
		return this;
	}

	private class DefaultEpsComputationPolicy implements EpsComputationPolicy {

		@Override
		public double compute() {
			double eps = 0.0;
			double maxDiff = 0.0;
			float pruneTailRate = 0.0F;
			int discardCount = (int) (allPoints.size() * pruneTailRate);
			int startIndex = 0;
			int endIndex = allPoints.size() - discardCount;
			LOG.info("Prune: pruneTailRate=" + pruneTailRate + ", startIndex=" + startIndex + ", endIndex=" + endIndex);
			
			KPoint2D kp1 = (KPoint2D) allPoints.get(startIndex);
			eps = kp1.kDistance;
			for(int i=startIndex + 1; i<endIndex; i++) {
				KPoint2D kp2 = (KPoint2D) allPoints.get(i);
				double diff = kp2.kDistance - kp1.kDistance;
				if(diff > maxDiff) {
					maxDiff = diff;
					eps = kp2.kDistance;
				}
				kp1 = kp2;
			}
			return eps;
		}
		
	}

	private KDistanceCalculator getCalculator() {
		int index = taskIndex++ % calculatorCount;
		return calculators.get(index);
	}

	private class KDistanceCalculator extends Thread {
		
		private final Log LOG = LogFactory.getLog(KDistanceCalculator.class);
		private final BlockingQueue<Task> q;
		
		public KDistanceCalculator(int qsize) {
			q = new LinkedBlockingQueue<Task>(qsize);
		}
		
		@Override
		public void run() {
			try {
				while(!completeToAssignTask) {
					try {
						while(!q.isEmpty()) {
							Task task = q.poll();
							KPoint2D p1 = (KPoint2D) task.p;
							final TreeSet<Double> sortedDistances = Sets.newTreeSet(new Comparator<Double>() {

								@Override
								public int compare(Double o1, Double o2) {
									double diff = o1 - o2;
									if(diff > 0) {
										return -1;
									}
									if(diff < 0) {
										return 1;
									}
									return 0;
								}
								
							});
							for (int i = 0; i < allPoints.size(); i++) {
								if(task.pos != i) {
									KPoint2D p2 = (KPoint2D) allPoints.get(i);
									Set<Point2D> set = Sets.newHashSet((Point2D) p1, (Point2D) p2);
									Double distance = distanceCache.getIfPresent(set);
									if(distance == null) {
										distance = MetricUtils.euclideanDistance(p1, p2);
										distanceCache.put(set, distance);
									}
									if(!sortedDistances.contains(distance)) {
										sortedDistances.add(distance);
									}
									if(sortedDistances.size() > k) {
										Iterator<Double> iter = sortedDistances.iterator();
										iter.next();
										// remove (k+1)th minimum distance
										iter.remove();
									}
								}
							}
							
							// collect k-distance
							p1.kDistance = sortedDistances.iterator().next();
							LOG.debug("Processed, point=(" + p1 + "), k-distance=" + p1.kDistance);
						}
						Thread.sleep(100);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			} finally {
				latch.countDown();
				LOG.info("k-distance calculator exited: " + this);
			}
		}
		
	}
	
	private class Task {
		
		private final Point2D p;
		private final int pos;
		
		public Task(Point2D p, int pos) {
			super();
			this.p = p;
			this.pos = pos;
		}
	}
	
	public double getDistance(Set<Point2D> pointPair) {
		return distanceCache.getIfPresent(pointPair);
	}
	
	/**
	 * k-distance point
	 * 
	 * @author yanjun
	 */
	private class KPoint2D extends Point2D {

		private Double kDistance = 0.0;
		
		public KPoint2D(Double x, Double y) {
			super(x, y);
		}
		
	}
	
	public double getSelectedKDistance() {
		return selectedKDistance;
	}
	
}
