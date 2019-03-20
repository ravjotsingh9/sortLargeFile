package sortservice.split;

import java.util.StringTokenizer;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemProcessor;

import sortservice.model.Line;


public class LineProcessor implements ItemProcessor<Line, Line>, StepExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(LineProcessor.class);
 
    @Override
    public void beforeStep(StepExecution stepExecution) {
        log.debug("Line Processor initialized.");
    }

    /**
	 * Creates a Line object with words filled in a Treeset 
     * @param Line contains string received from reader
	 * @return a Line
	 */
    @Override
    public Line process(final Line line) throws Exception {
        final String ln = line.getLine().toLowerCase();
        TreeSet<String> set = new TreeSet<String>();
        StringTokenizer st = new StringTokenizer(ln, " ,.;:\"");
        while (st.hasMoreTokens()) {
            String tmp = st.nextToken();
            if (!set.contains(tmp)) {
                set.add(tmp);
            }
        }

        final Line transformedLine = new Line(ln, set);

        //log.info("Converting (" + ln + ") into (" + transformedLine + ")");

        return transformedLine;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        log.debug("Line Processor ended.");
        return ExitStatus.COMPLETED;
    }

}
