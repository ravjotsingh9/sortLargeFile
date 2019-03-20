package sortservice.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import sortservice.merge.FilesMergeProcessor;
import sortservice.model.Line;
import sortservice.listener.JobCompletionNotificationListener;
import sortservice.split.LineProcessor;
import sortservice.split.LineReader;
import sortservice.split.LinesWriter;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    
    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    /**
    * Creates a reader to read lines from inputfile
    * @return an item reader to read input file
    */
    @Bean
    public ItemReader<Line> lineReader() {
        return new LineReader();
    }

    /**
    * Creates a processor to process lines read from inputfile
    * @return a lineprocessor to process the content in memory
    */
    @Bean
    public LineProcessor lineProcessor() {
        return new LineProcessor();
    }
    /**
    * Creates a writer to write lines received from processor
    * @return an item writer to write processed to output file
    */
    @Bean
    public ItemWriter<Line> lineWriter() {
        return new LinesWriter();
    }

    /**
    * Creates a merger processor to merge the intermediate files into 
    * one output file
    * @return a FilesMergerProcessor
    */
    @Bean
    public FilesMergeProcessor mergeProcessor() {

        return new FilesMergeProcessor();
    }

    /**
    * Provides a task executor to enable multithreading in a batch step 
    * @return a TaskExecutor
    */
    // @Bean
    // public TaskExecutor taskExecutor() {
    //     SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
    //     taskExecutor.setConcurrencyLimit(5);
    //     return taskExecutor;
    // }

    /**
    * Creates a job that spring container shall run
    * @return a Job
    */
    @Bean
    public Job sortLargeFile(JobCompletionNotificationListener listener) {
        return jobBuilderFactory.get("SortLargFile")
            .incrementer(new RunIdIncrementer())
            .listener(listener)
            .flow(split())
            .next(mergeFiles())
            .end()
            .build();
    }

    /**
    * Creates a step to read a chunk of input file,
    * process it and write it to disk
    * @return a Step
    */
    @Bean
    public Step split() {
        return stepBuilderFactory.get("Split")
            .<Line, Line> chunk(Config.CHUNK_SIZE_IN_TOTAL_LINES)
            .reader(lineReader())
            .processor(lineProcessor())
            .writer(lineWriter())
            .build();
    }

    /**
    * Creates a step to merge the intermediate file
    * generated in split step
    * @return a Step
    */
    @Bean
    public Step mergeFiles() {
        return stepBuilderFactory.get("MergeSortedFiles")
            .tasklet(mergeProcessor())
            .build();
    }
}
