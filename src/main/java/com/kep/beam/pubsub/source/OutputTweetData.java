package com.kep.beam.pubsub.source;

import java.io.Serializable;
import java.util.List;

import lombok.Value;

@Value(staticConstructor = "of")
public class OutputTweetData implements Serializable {

    private String username;
    private List<String> tweets;
}
