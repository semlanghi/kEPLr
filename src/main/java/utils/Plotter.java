package utils;

import com.opencsv.CSVReader;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Iterator;

public class Plotter extends ApplicationFrame {

    public static void main(final String[] args) throws FileNotFoundException {

        final Plotter demo = new Plotter("XY Series Demo");
        demo.pack();

        RefineryUtilities.centerFrameOnScreen(demo);
        demo.setVisible(true);

    }

    private Plotter(final String title) throws FileNotFoundException {

        super(title);
        final XYSeries series = new XYSeries("Random Data");

        CSVReader reader = new CSVReader(new FileReader("src/main/resources/output_final_esper_part_4_output.csv"));
        Iterator<String[]> it = reader.iterator();
        it.next();
        String[] line;
        while(it.hasNext()){
            line = it.next();
            series.add(Double.parseDouble(line[0])/1000, Double.parseDouble(line[1])*1000);
        }

        final XYSeriesCollection data = new XYSeriesCollection(series);
        final JFreeChart chart = ChartFactory.createXYLineChart(
                "Esper WO Context Output Throughput",
                "Time [s]",
                "Throughput [event/s]",
                data,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        final ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new java.awt.Dimension(500, 270));
        setContentPane(chartPanel);

    }


}
