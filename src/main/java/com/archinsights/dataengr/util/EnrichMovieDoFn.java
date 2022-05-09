package com.archinsights.dataengr.util;

import com.archinsights.dataengr.domain.MovieRating;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.UUID;

public class EnrichMovieDoFn extends DoFn <String, MovieRating>{

    private static final Logger appLogger = LoggerFactory.getLogger(EnrichMovieDoFn.class);


    public void startBundle(){

        //Instantiate your external service client (Static if threadsafe)

    }
    @ProcessElement
    public void processElement(@Element String input, OutputReceiver<MovieRating> outBoundGenerator){ //@Element String word, OutputReceiver<Integer> out

        // Call out to external service
        appLogger.info("string element>>" + input.split(",")[0]);

        appLogger.info("string element>>" + input.split(",")[1]);

        String id = input.split(",")[0];

        String movieName = input.split(",")[1];

        Integer rating = generateRating();

        //Call out to external service

        String dataId = simulateAPICall(id);

        MovieRating movieRatingData = new MovieRating(input,rating);

        movieRatingData.setId(dataId);
        movieRatingData.setMovieName(movieName);

        outBoundGenerator.output(movieRatingData);

    }

    public void finishBundle(){

        //Shutdown your external service client if needed

    }//finishBundle

    private String simulateAPICall(String input)
    {

        appLogger.info("simulateAPICall method is invoked>>");

        UUID uuid = UUID.randomUUID();

        return uuid.toString();

    }


    private Integer generateRating()
    {

        Integer rating = 0;

        Random randomGenerator = new Random();

        rating = randomGenerator.nextInt(5);

        return rating;

    }

}//end class