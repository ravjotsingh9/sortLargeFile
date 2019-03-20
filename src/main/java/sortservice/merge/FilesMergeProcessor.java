package sortservice.merge;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import sortservice.config.Config;
@Component
public class FilesMergeProcessor implements Tasklet, StepExecutionListener {

    private Logger logger = LoggerFactory.getLogger(FilesMergeProcessor.class);
    
    private List<FlatFileItemReader<String>> readers;

    private FlatFileItemWriter<String> writer;

    private static final int DEFAULT_MAX_ROWS_READ = 100000;  
    private int maxRecords;

    public FilesMergeProcessor(){
        this.maxRecords = this.DEFAULT_MAX_ROWS_READ;
        this.readers = new ArrayList<FlatFileItemReader<String>>();
     
    }
    
    /**
    * Creates a FlatFileItemReader for the provided file resource
    * @param in file to which reader is required
    * @return a reader
    */
    public FlatFileItemReader<String> fileReader( Resource in) throws Exception {

        return new FlatFileItemReaderBuilder<String>().name("file-reader").resource(in)
                .lineMapper(new LineMapper<String>(){
                
                    @Override
                    public String mapLine(String line, int lineNumber) throws Exception {
                        return line;
                    }
                })
                .build();
    }

    /**
    * Creates a FlatFileItemWriter for the provided file resource
    * @param resource file to which writer is required
    * @return a writer
    */
    public FlatFileItemWriter<String> fileWriter( Resource resource) {
        return new FlatFileItemWriterBuilder<String>()
                .name("file-writer")
                .resource(resource)
                .lineAggregator(new LineAggregator<String>(){
                
                    @Override
                    public String aggregate(String item) {
                        return item;
                    }
                })
                .build();
    }

    @Override
    public synchronized void beforeStep(StepExecution stepExecution) {
        logger.debug("Lines Processor initialized.");

        
        // Read the list of intermediate
        File folder = new File(Config.DESTINATION_LOCATION);
        File[] listOfFiles = folder.listFiles();

        // Go over the list of files and add them to readers obj
        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) {
                FlatFileItemReader<String> fReader;
                try {
                    fReader = fileReader(new FileSystemResource(listOfFiles[i]));
                    readers.add(fReader);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            } 
          }

        // Create writer for the output file
        this.writer= fileWriter(new FileSystemResource(Config.DESTINATION_FILE_LOCATION));
       
    }

     @Override
     public synchronized RepeatStatus execute(StepContribution stepContribution, 
       ChunkContext chunkContext) throws Exception {

        // Execution context
        ExecutionContext context = new ExecutionContext();
        
        // Read first line from each file into the memory
	logger.info("Reading first line of each intermediate file");
        Map<FlatFileItemReader<String>, String> firstLines = new HashMap<FlatFileItemReader<String>, String>();
        for (FlatFileItemReader<String> r : readers) {
		r.open(context);
		firstLines.put(r, r.read());
	}


        // Open output file for writing
	writer.setTransactional(false);
	writer.open(context);

        // List to contain all the write content before dumping to file
        List<String> writeList = new ArrayList<String>();
        
        // Create a local copy of all reader
	List<FlatFileItemReader<String>> readers = new ArrayList<FlatFileItemReader<String>>();
        readers.addAll(this.readers);
        

        // Variable to avoid writing a word if it is already written
        String prevWinner = "";

	while (readers.size() > 0) {
		int numReaders = readers.size();
            
		// Assume first reader is winner
		FlatFileItemReader<String> winningReader = readers.get(0);
            	String winner = firstLines.get(winningReader);
            
            	// Traverse over other readers to see which one is real winner
		for (int i = 1; i < numReaders; i++) {
			String current = firstLines.get(readers.get(i));
			if (winner.compareTo(current) > 0) {
				winner = current;
				winningReader = readers.get(i);
			}
		}
            
            	// If only, the current winner word is different from previous then only add it to writelist
		if(winner.compareTo(prevWinner) != 0){  
                	if (writeList.size() < maxRecords) {
                    		writeList.add(winner);   
                	} else {
				writer.write(writeList);
				writeList = new ArrayList<String>();
				System.gc();
				writeList.add(winner);
                	}
                	prevWinner = winner;
           	}  
		// fetch next line for the winner reader
		String next = winningReader.read();
		if (next == null) {
			winningReader.close();
			readers.remove(winningReader);
		} else {
			firstLines.put(winningReader, next);
		}
	}
		
	// Write remaining to output
	if (writeList.size() > 0) {
		writer.write(writeList);
	}
		
	// Close writer
	writer.close();
		
        return RepeatStatus.FINISHED;
       }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        return ExitStatus.COMPLETED;
    }




}
