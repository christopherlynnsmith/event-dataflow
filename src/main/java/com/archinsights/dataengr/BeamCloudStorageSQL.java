package com.archinsights.dataengr;

import com.archinsights.dataengr.domain.MovieRating;
import com.archinsights.dataengr.util.EnrichMovieDoFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;

/**
 *  Resources
 *  dataflow gradle - https://itnext.io/creating-a-simple-cloud-dataflow-with-kotlin-cc9f76f47bc5
 *
 *  dataflow gradle dependencies
 *
 *  cmd line args -   https://i101330.hatenablog.com/entry/2018/04/25/124127
 *
 *  user defined function - https://medium.com/google-cloud/setting-up-a-java-development-environment-for-apache-beam-on-google-cloud-platform-ec0c6c9fbb39
 *
 *  user defined javascript https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/master/src/main/java/com/google/cloud/teleport/templates/TextIOToBigQuery.java
 *      see line 99
 *
 *  specifiying cmd execution parameters https://cloud.google.com/dataflow/docs/guides/specifying-exec-params
 *
 *  maven setup  https://medium.com/google-cloud/setting-up-a-java-development-environment-for-apache-beam-on-google-cloud-platform-ec0c6c9fbb39
 */
public class BeamCloudStorageSQL {

    private static final Logger appLogger = LoggerFactory.getLogger(BeamCloudStorageSQL.class);

    public interface MyPipelineOptions extends PipelineOptions {

        boolean usePublicIps = true;

        String network = "default";

        @Validation.Required
            // @Default.String("com.mysql.jdbc.Driver")
        ValueProvider<String>  getJdbcDriver();
        void setJdbcDriver( ValueProvider<String> jdbcDriver);

        //cloud storage bucket
        //@Default.String("gs://data-flow-sample-data-kurisu/data/data-*.txt")
        @Validation.Required // this was commented out
        ValueProvider<String>  getDataPath();
        void setDataPath( ValueProvider<String> dataPath);

        @Validation.Required
        ValueProvider<String>  getJdbcUrl();
        void setJdbcUrl( ValueProvider<String>  jdbcUrl);

        @Validation.Required
        ValueProvider<String>  getUsername();

        void setUsername( ValueProvider<String>  username);

        @Validation.Required
        ValueProvider<String>  getPassword();

        void setPassword( ValueProvider<String>  password);

    }


    /**
     *
     * @param args
     */
    public static void main(String[] args) {

        appLogger.info("###################################################");

        PipelineOptionsFactory.register(MyPipelineOptions.class);

        MyPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(MyPipelineOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        // Step 1 - Read CSV file.
        //PCollection<String> csvRows = pipeline.apply("Read from CSV", TextIO.read().from("./data*.txt"));


        PCollection<String> csvRows  = pipeline.apply("ReadTextFromFiles",
                TextIO.read().from(options.getDataPath()));

        PCollection<MovieRating> movieRows  = csvRows.apply("Construct MovieRating Objects", ParDo.of(new EnrichMovieDoFn() ));

        //String outputPath = new File("src/main/resources/beam_output/").getPath();


        appLogger.info("pcollection movieRows is bounded>>" + movieRows.isBounded());

      /*

            distributed nature of beam so inserts and updates should ignore errors
            for mysql
            INSERT IGNORE https://www.mysqltutorial.org/mysql-insert-ignore/
            UPDATE IGNORE https://www.mysqltutorial.org/mysql-update-data.aspx

            THIS EXAMPLE USES INSERT IGNORE
         */

        
        // Run the pipeline and wait till it finishes before exiting
        pipeline.run().waitUntilFinish();
    }


    private static void setPipelineOptions( PipelineOptions options)
    {
        //DataflowPipelineWorkerPoolOptions workerPoolOptions = options.as(DataflowPipelineWorkerPoolOptions.class);
        //workerPoolOptions.setNetwork("default");

    }

//pipeline enricher options - read user name from cmd line
//    https://medium.com/@tnymltn/enrichment-pipeline-patterns-using-apache-beam-4b9b81e5d9f3

//parse text into key value pairs
// https://engineering.universe.com/building-a-data-warehouse-using-apache-beam-and-dataflow-part-i-building-your-first-pipeline-b63d22c86662

    static class SQLStatementSetter implements JdbcIO.PreparedStatementSetter<MovieRating>
    {
        private static final long serialVersionUID = 1L;


        /**
         *
         * @param element
         * @param query
         * @throws Exception
         */
        public void setParameters(MovieRating element, PreparedStatement query) throws Exception
        {
/*
            System.out.println("string element>>" + element.split(",")[0]);
            System.out.println("string element>>" + element.split(",")[1]);
            System.out.println("string element>>" + element.split(",")[2]);
*/
            query.setString(1, element.getId());
            query.setString(2, element.getMovieName());
            query.setInt(3, element.getRating() );


        }
    }

}
