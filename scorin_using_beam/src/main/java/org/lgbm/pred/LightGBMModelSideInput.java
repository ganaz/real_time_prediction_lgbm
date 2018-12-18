package org.lgbm.pred;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.ModelEvaluator;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.model.PMMLUtil;
import org.jpmml.model.visitors.LocatorTransformer;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by gpatil on 12/17/2018.
 */
public class LightGBMModelSideInput implements Serializable {


    public PCollectionView<List<ModelEvaluator>> getModelIdList(Pipeline pipeline, String modelFilePath) {

        createSideInput(pipeline, modelFilePath);

        return modelIdList;
    }

    private PCollectionView<List<ModelEvaluator>> modelIdList;

    private void createSideInput(Pipeline pipeline, String modelFilePath) {

        PMML pmml = null;
        try {
            pmml = PMMLUtil.unmarshal(new FileInputStream(new File(modelFilePath)));


            LocatorTransformer locatorTransformer = new LocatorTransformer();
            locatorTransformer.applyTo(pmml);
            ModelEvaluator<?> modelEvaluator = ModelEvaluatorFactory.newInstance().newModelEvaluator(pmml);

            modelEvaluator.verify();

            ArrayList<ModelEvaluator> al = new ArrayList<ModelEvaluator>();
            al.add(modelEvaluator);


            PCollection<ModelEvaluator> apply = pipeline.apply("CreateFromList", Create.of(al));


            modelIdList = apply.apply("CreateSideInput", View.<ModelEvaluator>asList());


        } catch (SAXException e) {
            e.printStackTrace();
        } catch (JAXBException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }


    }


}
