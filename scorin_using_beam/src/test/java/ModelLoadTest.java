import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.dmg.pmml.tree.TreeModel;
import org.jpmml.evaluator.InputField;
import org.jpmml.evaluator.ModelEvaluator;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.model.PMMLUtil;
import org.jpmml.model.visitors.LocatorTransformer;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;

/**
 * Created by gpatil on 12/17/2018.
 */
public class ModelLoadTest {


    @Test
    public void testModel() {

        String modelFilePath = "C:\\study\\doc_code\\model_training\\pmml\\model.pmml";

        try {
            PMML pmml = PMMLUtil.unmarshal(new FileInputStream(new File(modelFilePath)));

            LocatorTransformer locatorTransformer = new LocatorTransformer();
            locatorTransformer.applyTo(pmml);


            ModelEvaluator<?> modelEvaluator = ModelEvaluatorFactory.newInstance().newModelEvaluator(pmml);

            modelEvaluator.verify();

            //String data = "0.644,0.247, -0.447,0.862,0.374,0.854, -1.126, -0.79 ,2.173,1.015, -0.201,1.4,0.,1.575,1.807,1.607,0.,1.585, -0.19 , -0.744,3.102,0.958,1.061,0.98 ,0.875,0.581,0.905,0.796";
            String data = "1.392,-0.358,0.235,1.494,-0.461,0.895,-0.848,1.549,2.173,0.841,-0.384,0.666,1.107,1.199,2.509,-0.891,0.,1.109,-0.364,-0.945,0.,0.693,2.135,1.17,1.362,0.959,2.056,1.842";

            String header = "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28";

            HashMap<String, String> hm = new HashMap<String, String>();


            String[] dataSplit = data.split(",");
            String[] headerSplit = header.split(",");

            List<InputField> inputFields = modelEvaluator.getInputFields();


            for (int i = 0; i < dataSplit.length; i++) {

                hm.put(headerSplit[i], dataSplit[i]);

            }

            HashMap<FieldName, String> eval = new HashMap<FieldName, String>();

            for (InputField inputField : inputFields) {

                System.out.println(inputField.getName().getValue());
                eval.put(inputField.getName(), hm.get(inputField.getName().getValue()));
            }


            assert 0.600f == Float.parseFloat(modelEvaluator.evaluate(eval).get(FieldName.create("probability(1)")).toString().substring(0, 4));


        } catch (SAXException e) {
            e.printStackTrace();
        } catch (JAXBException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }
}
