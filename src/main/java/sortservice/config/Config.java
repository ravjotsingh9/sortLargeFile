package sortservice.config;

public class Config {
    /**
	 * Number of newline separated lines to be read at once into memory
     * Be careful here, too big choice can cause thrashing, hence bad performance
	 */
    public static final Integer CHUNK_SIZE_IN_TOTAL_LINES = 2500000;


    /**
	 * Text input file
     * 
	 */
    public static final String SOURCE_FILE_PATH = "./src/main/resources/big.txt";

    /**
	 * Output file location. The directory path must exist.
	 */
    public static final String DESTINATION_FILE_LOCATION = "./output/out.full";

    /**
	 * Intermediate file location.The directory path must exist.
	 */
    public static final String DESTINATION_LOCATION = "./output/";
}