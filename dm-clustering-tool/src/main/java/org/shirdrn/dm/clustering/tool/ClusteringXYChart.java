package org.shirdrn.dm.clustering.tool;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.EventQueue;
import java.awt.Font;
import java.awt.HeadlessException;
import java.awt.event.ActionEvent;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import javax.swing.AbstractAction;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.chart.title.TextTitle;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.HorizontalAlignment;
import org.jfree.ui.RectangleEdge;
import org.jfree.ui.RectangleInsets;
import org.jfree.ui.VerticalAlignment;
import org.shirdrn.dm.clustering.common.ClusterPoint2D;
import org.shirdrn.dm.clustering.common.utils.FileUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ClusteringXYChart extends JFrame {

	private static final long serialVersionUID = 1L;
	private String chartTitle;
	private File clusterPointFile;
	private Map<Integer, Set<ClusterPoint2D>> clusterPoints;
	private final Set<ClusterPoint2D> noisePoints = Sets.newHashSet();
	private int noisePointsClusterId;
	private XYSeries noiseXYSeries;
	private Color noisePointColor;
	private final List<Color> colorSpace = Lists.newArrayList();
	private final Random rand = new Random();
	
	public ClusteringXYChart(String chartTitle) throws HeadlessException {
		super();
		this.chartTitle = chartTitle;
	}
	
	private XYSeriesCollection buildXYDataset() {
		XYSeriesCollection xySeriesCollection = new XYSeriesCollection();
		clusterPoints = createClusterPoints(clusterPointFile);
		
		Iterator<Entry<Integer, Set<ClusterPoint2D>>> iter = clusterPoints.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<Integer, Set<ClusterPoint2D>> entry = iter.next();
			int clusterId = entry.getKey();
			Set<ClusterPoint2D> clusteredPoints = entry.getValue();
			XYSeries xySeries = new XYSeries(clusterId);
			for(ClusterPoint2D p : clusteredPoints) {
				xySeries.add(p.getX(), p.getY());
			}
			xySeriesCollection.addSeries(xySeries);
		}
		return xySeriesCollection;
	}
	
	private void setNoisePointColor() {
        if(noisePointColor == null && !colorSpace.isEmpty()) {
        	noisePointColor = selectColorRandomly();
        }
	}
	
	public Map<Integer, Color> generateColors(Set<Integer> clusterIdSet) {
		Map<Integer, Color> colors = Maps.newHashMap();
		int numClusters = clusterIdSet.size();
		List<Integer> rValues = Lists.newArrayList();
		List<Integer> gValues = Lists.newArrayList();
		List<Integer> bValues = Lists.newArrayList();
		int i = 1;
		for(; i<Integer.MAX_VALUE; i++) {
			int product = i * i * i;
			if(product > numClusters * 3 + i) {
				break;
			}
		}
		
		int step = 255 / i;
		generateColorValues(step, rValues);
		generateColorValues(step, gValues);
		generateColorValues(step, bValues);
		
		for (int rIndex = 0; rIndex < rValues.size(); rIndex++) {
			for (int gIndex = 0; gIndex < rValues.size(); gIndex++) {
				for (int bIndex = 0; bIndex < rValues.size(); bIndex++) {
					colorSpace.add(new Color(rValues.get(rIndex), gValues.get(gIndex), bValues.get(bIndex)));
				}
			}
		}
		
		for (int clusterId : clusterIdSet) {
			Color color = selectColorRandomly();
			colors.put(clusterId, color);
		}
		return colors;
	}
	
	private Color selectColorRandomly() {
		int index = rand.nextInt(colorSpace.size());
		return colorSpace.remove(index);
	}
	
	
	private void generateColorValues(int step, List<Integer> rgbValues) {
		for (int j = 0; j < 255; j+=step) {
			rgbValues.add(j);
		}		
	}

	private Map<Integer, Set<ClusterPoint2D>> createClusterPoints(File pointFile) {
		Map<Integer, Set<ClusterPoint2D>> points = Maps.newHashMap();
		BufferedReader reader = null;
		try {
			// collect clustered points
			reader = new BufferedReader(new FileReader(pointFile.getAbsoluteFile()));
			String line;
			while((line = reader.readLine()) != null) {
				if(!line.trim().isEmpty()) {
					String[] a = line.split("[,;\t\\s]+");
					if(a.length == 3) {
						int clusterId = Integer.parseInt(a[2]);
						ClusterPoint2D clusterPoint = 
								new ClusterPoint2D(Double.parseDouble(a[0]), Double.parseDouble(a[1]), clusterId);
						// collect noise points
						if(clusterId == -1) {
							noisePoints.add(clusterPoint);
							continue;
						}
						Set<ClusterPoint2D> set = points.get(clusterId);
						if(set == null) {
							set = Sets.newHashSet();
							points.put(clusterId, set);
						}
						set.add(clusterPoint);
					}
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			FileUtils.closeQuietly(reader);
		}
		
		// set virtual noise cluster id
		noisePointsClusterId = points.size() + 1;
				
		return points;
	}
	
	public void renderXYChart() {
		// create xy dataset from points file
		final XYSeriesCollection xyDataset = buildXYDataset();
		noiseXYSeries = new XYSeries(noisePointsClusterId);
		xyDataset.addSeries(noiseXYSeries);
		
		// create chart & configure xy plot
		JFreeChart jfreechart = ChartFactory.createScatterPlot(null, "X", "Y", xyDataset, PlotOrientation.VERTICAL, true, true, false);
		TextTitle title = new TextTitle(chartTitle, new Font("Lucida Console", Font.BOLD, 16), 
				Color.DARK_GRAY, RectangleEdge.TOP, HorizontalAlignment.CENTER, 
				VerticalAlignment.TOP, RectangleInsets.ZERO_INSETS);
		jfreechart.setTitle(title);
		XYPlot xyPlot = (XYPlot) jfreechart.getPlot();
		xyPlot.setDomainCrosshairVisible(true);
		xyPlot.setRangeCrosshairVisible(true);
		
		// render clustered series
		final XYItemRenderer renderer = xyPlot.getRenderer();
		Map<Integer, Color> colors = generateColors(clusterPoints.keySet());
		Iterator<Entry<Integer, Color>> iter = colors.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<Integer, Color> entry = iter.next();
			renderer.setSeriesPaint(entry.getKey(), entry.getValue());
		}
		
		// render noise series
		setNoisePointColor();
		renderer.setSeriesPaint(noisePointsClusterId, noisePointColor);
		
		NumberAxis domain = (NumberAxis) xyPlot.getDomainAxis();
		domain.setVerticalTickLabels(true);
		
		final ChartPanel chartPanel = new ChartPanel(jfreechart);
		this.add(chartPanel, BorderLayout.CENTER);
        
		JPanel panel = new JPanel();
        
        // display noise points
        panel.add(new JButton(new AbstractAction("Display Noise Points") {
			private static final long serialVersionUID = 1L;
			@Override
            public void actionPerformed(ActionEvent e) {
				for(ClusterPoint2D p : noisePoints) {
					noiseXYSeries.add(p.getX(), p.getY());
				}
            }
        }));
        
        // hide noise points
        panel.add(new JButton(new AbstractAction("Hide Noise Points") {
			private static final long serialVersionUID = 1L;
			@Override
            public void actionPerformed(ActionEvent e) {
				noiseXYSeries.clear();
            }
        }));
        
        this.add(panel, BorderLayout.SOUTH);
	}
	
	public void setclusterPointFile(File clusterPointFile) {
		this.clusterPointFile = clusterPointFile;
	}
	
	private static File getClusterPointFile(String[] args, String dir, int minPts, String eps) {
		if(args.length > 0) {
			return new File(args[0]);
		}
		return new File(new File(dir), minPts + "_" + eps + ".txt");		
	}
	
	public static void main(String args[]) {
		int minPts = 4;
		String eps = "0.0025094814205335555";
//		String eps = "0.004417483559674606";
//		String eps = "0.005547485196013346";
//		String eps = "0.006147849217403014";
		
		String chartTitle = "DBSCAN [Eps=" + eps + ", minPts=" + minPts + "]";
		String dir = "C:\\Users\\yanjun\\Desktop";
		File clusterPointFile = getClusterPointFile(args, dir, minPts, eps);
		
		final ClusteringXYChart chart = new ClusteringXYChart(chartTitle);
		chart.setclusterPointFile(clusterPointFile);
		
        EventQueue.invokeLater(new Runnable() {

            @Override
            public void run() {
            	chart.renderXYChart();
                chart.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
                chart.pack();
                chart.setLocationRelativeTo(null);
                chart.setVisible(true);
            }
        });
    }

}
