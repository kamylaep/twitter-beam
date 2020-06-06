package com.kep.beam.pubsub.source;

import com.google.gson.annotations.SerializedName;

import lombok.Value;

@Value(staticConstructor = "of")
public class InputTweetData implements InputData {

    private String id;
    private String source;
    private String text;

    @SerializedName("user.id")
    private String userId;

}
