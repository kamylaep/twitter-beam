package com.kep.beam.pubsub;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;

import com.kep.beam.pipeline.TwitterBeanOptions;

public interface PubSubBeamOptions extends TwitterBeanOptions, GcpOptions {

}
