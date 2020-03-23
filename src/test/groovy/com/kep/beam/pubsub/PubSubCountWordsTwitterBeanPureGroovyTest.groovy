package com.kep.beam.pubsub


import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.transforms.Create
import org.junit.Rule
import org.junit.Test

class PubSubCountWordsTwitterBeanPureGroovyTest {

    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.fromOptions(buildOptions())

    def pubSubCountWordsTwitterBean = new PubSubCountWordsTwitterBean()

    @Test
    void "should count tweet words"() {
        def input = testPipeline.apply("Create input", Create.of(['{"text": "I am a tweet tweet"}', '{"text": "Another tweet"}']))
        def output = pubSubCountWordsTwitterBean.buildPipeline(buildOptions(), input)
        PAssert.that(output).containsInAnyOrder(['I:1', 'am:1', 'a:1', 'tweet:3', 'Another:1'])
        testPipeline.run().waitUntilFinish()
    }

    def buildOptions() {
        def pipelineOptions = TestPipeline.testingPipelineOptions().as(PubSubBeamOptions.class)
        pipelineOptions.windowInSeconds = 10
        return pipelineOptions
    }

}
