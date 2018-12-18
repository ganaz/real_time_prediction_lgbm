package org.lgbm.pred;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionView;
import org.dmg.pmml.FieldName;
import org.jpmml.evaluator.InputField;
import org.jpmml.evaluator.ModelEvaluator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Created by gpatil on 12/17/2018.
 */

public class LightGBMPredictioniPipeline {


    public static void main(String[] args) {

        String modelFilePath = args[0];


        DataflowPipelineOptions dataflowPipelineOptions = PipelineOptionsFactory.as(DataflowPipelineOptions.class);

        //dataflowPipelineOptions.setRunner(DirectRunner.class);
        dataflowPipelineOptions.setRunner(DataflowRunner.class);
        dataflowPipelineOptions.setProject("<project-id>");
        dataflowPipelineOptions.setStagingLocation("<staging location>");

        Pipeline pipeline = Pipeline.create(dataflowPipelineOptions);

        LightGBMModelSideInput lightGBMModelSideInput = new LightGBMModelSideInput();
        final PCollectionView<List<ModelEvaluator>> lightGBMModelSideInputModelIdList = lightGBMModelSideInput.getModelIdList(pipeline, modelFilePath);

        //String sampleData = "1.392,-0.358,0.235,1.494,-0.461,0.895,-0.848,1.549,2.173,0.841,-0.384,0.666,1.107,1.199,2.509,-0.891,0.,1.109,-0.364,-0.945,0.,0.693,2.135,1.17,1.362,0.959,2.056,1.842";

        pipeline.apply("ReadPubSub", PubsubIO.readStrings().fromSubscription("<subscription-name>"))
        //pipeline.apply("Create", Create.of(sampleData))
                .apply("scoreTransform", ParDo.of(new DoFn<String, String>() {

                    @ProcessElement
                    public void processEle(@Element String string, OutputReceiver<String> outputReceiver, ProcessContext processContext) {
                        List<String> dataList = Arrays.asList(string.split(","));



                        List<ModelEvaluator> modelEvaluators = processContext.sideInput(lightGBMModelSideInputModelIdList);

                        ModelEvaluator modelEvaluator = modelEvaluators.get(0);
                        List inputFields = modelEvaluator.getInputFields();
                        HashMap<FieldName, String> hm = new HashMap<>();
                        for (Object inputField : inputFields) {
                            InputField inputField1 = (InputField) inputField;
                            FieldName name = inputField1.getName();

                            // As our feature name is from 1 to 28 , we can use it as index to get the values from dataList,
                            // If we use different feature names to train the model then we need to add another sideinput containing sequence of field name.
                            hm.put(name, dataList.get(Integer.parseInt(name.getValue()) - 1));
                        }

                        String score = modelEvaluator.evaluate(hm).get(FieldName.create("probability(1)")).toString().substring(0, 4);

                        outputReceiver.output(score);

                    }
                }).withSideInputs(lightGBMModelSideInputModelIdList))
        .apply("Save", PubsubIO.writeStrings().to("<topic name>"))
        ;

        pipeline.run();

    }

}
