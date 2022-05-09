package com.archinsights.dataengr.domain;

import java.io.Serializable;

public class MovieRating implements Serializable {

    private String movieName;

    private Integer rating;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    private String id;


    public MovieRating(String movieName, Integer rating)
    {

        this.movieName = movieName;
        this.rating = rating;
    }


    public String getMovieName() {
        return movieName;
    }

    public void setMovieName(String movieName) {
        this.movieName = movieName;
    }

    public Integer getRating() {
        return rating;
    }

    public void setRating(Integer rating) {
        this.rating = rating;
    }




}
