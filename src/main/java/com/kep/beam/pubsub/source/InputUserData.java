package com.kep.beam.pubsub.source;

import com.google.gson.annotations.SerializedName;

import lombok.Value;

@Value(staticConstructor = "of")
public class InputUserData implements InputData {

    @SerializedName("user.id")
    private String userId;
    @SerializedName("user.name")
    private String name;
    @SerializedName("user.screen_name")
    private String screenName;
    @SerializedName("user.friends_count")
    private Integer friendsCount;
    @SerializedName("user.followers_count")
    private Integer followersCount;
}
