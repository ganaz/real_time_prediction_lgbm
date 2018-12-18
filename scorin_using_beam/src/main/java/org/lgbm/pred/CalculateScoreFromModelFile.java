package org.lgbm.pred;

import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Created by gpatil on 10/8/2017.
 * <p>
 * Utility class for calculate score from model file
 */
public class CalculateScoreFromModelFile {

    private static final Logger LOGGER = LoggerFactory.getLogger(CalculateScoreFromModelFile.class);

    private static HashMap<String, HashMap<String, ArrayList<ArrayList<?>>>> modelIdModelDetailsMap = new HashMap<>();

    public static HashMap<String, ArrayList<String>> modelIdFeatureNameMap = new HashMap<>();


    public static void main(String[] args) throws IOException, GeneralSecurityException {


        String modelDir = args[0];

        List<Path> modelFileList = null;

        try (Stream<Path> paths = Files.walk(Paths.get(modelDir))) {
            modelFileList = paths
                    .filter(Files::isRegularFile)
                    .collect(Collectors.toList());
        }

        populateModelData(modelFileList);

        System.out.println("Size of modelFeatures :" + modelIdFeatureNameMap.size());
        System.out.println("Size of models Map:" + modelIdModelDetailsMap.size());

        System.out.println(" ModelFeatures :" + modelIdFeatureNameMap);
//        System.out.println(" ModelFeatures :" + modelIdModelDetailsMap);

        long millis1 = Instant.now().getMillis();
        String d = "1.392,-0.358,0.235,1.494,-0.461,0.895,-0.848,1.549,2.173,0.841,-0.384,0.666,1.107,1.199,2.509,-0.891,0.,1.109,-0.364,-0.945,0.,0.693,2.135,1.17,1.362,0.959,2.056,1.842";
        String[] split = d.split(",");

        ArrayList<Double> data = new ArrayList<>();
        for (int i = 0; i < split.length; i++) {

            data.add(Double.parseDouble(split[i]));
        }


        double score = getScore("model", data);
        long millis2 = Instant.now().getMillis();
        System.out.println("score :" + score + " with time :" + (millis2 - millis1) + " millis");

    }


    /**
     * This method populates the model details and model features maps by reading model txt files from
     * cloud storage.
     *
     * @throws IOException
     * @throws GeneralSecurityException
     */
    public static void populateModelData(List<Path> modelFileNameList) throws IOException, GeneralSecurityException {

        for (Path name : modelFileNameList) {

            BufferedReader bufferedReader = new BufferedReader(new FileReader(name.toFile()));


            String fileName = name.getFileName().toString().split("\\.")[0];

            // This list contains the list of information of trees.
            ArrayList<ArrayList<String>> model = new ArrayList<>();

            // this list contains the each information of each tree in model file.
            ArrayList<String> tree = new ArrayList<>();

            String line = null;
            while ((line = bufferedReader.readLine()) != null) {


                if (line.startsWith("feature_names")) {
                    String[] split1 = line.split("=")[1].split(" ");
                    ArrayList<String> featureNames = new ArrayList<>();
                    for (int i = 0; i < split1.length; i++) {

                        featureNames.add(split1[i].trim().toUpperCase());
                    }
                    modelIdFeatureNameMap.put(fileName, featureNames);
                } else {

                    if (!line.startsWith("Tree=")) {
                        tree.add(line.trim().replace("\n", ""));
                    } else {
                        if (tree.contains("tree")) {
                            tree.clear();
                            continue;
                        }
                        model.add(tree);
                        tree = new ArrayList<>();
                    }

                }
            }


            model.add(tree);

            HashMap<String, ArrayList<ArrayList<?>>> modelDetails = getModelDetails(model);

            model.clear();
            tree.clear();

            modelIdModelDetailsMap.put(fileName, modelDetails);
            LOGGER.debug("Model Details map :" + modelDetails);

            bufferedReader.close();

        }
    }


    /**
     * This is main method to get the score from model files.
     *
     * @param modelName
     * @param data      : Requires the data in the same order as the features used by models in model txt files.
     * @return : calculated score.
     */
    public static double getScore(String modelName, ArrayList<Double> data) {

        HashMap<String, ArrayList<ArrayList<?>>> modelDetailsMap = modelIdModelDetailsMap.get(modelName);
        if (modelDetailsMap == null) {
            LOGGER.error("Model file is not found in cloud storage for model id : " + modelName);
            return 0;
        }

        return calculateScore(modelDetailsMap, data, modelName);

    }


    /**
     * Calculates the score ...
     *
     * @param modelDetails
     * @param data
     * @param modelName
     * @return
     */
    private static double calculateScore(HashMap<String, ArrayList<ArrayList<?>>> modelDetails, ArrayList<Double> data, String modelName) {

        int numTrees = modelDetails.get("split_feature").size();
        double score = 0;
        try {
            for (int i = 0; i < numTrees; i++) {

                int node = 0;
                while (node >= 0) {

                    if (data.get((Integer) modelDetails.get("split_feature").get(i).get(node)) <= (Float) modelDetails.get("threshold").get(i).get(node)) {
                        node = (Integer) modelDetails.get("left_child").get(i).get(node);
                    } else {
                        node = (Integer) modelDetails.get("right_child").get(i).get(node);
                    }

                    if (node < 0) {

                        score = score + (Double) modelDetails.get("leaf_value").get(i).get(Math.abs(node) - 1);

                    }
                }

            }
        } catch (IndexOutOfBoundsException iob) {

            LOGGER.error("Error while calculating score for modelId : " + modelName + " data : " + data, iob);
            return 0;

        }
        double sigmoid = 1.0;

        return 1.0 / (1.0 + Math.exp(-1.0 * sigmoid * score));
    }


    /**
     * This method populates the required fields from model data.
     *
     * @param model
     * @return
     */
    private static HashMap<String, ArrayList<ArrayList<?>>> getModelDetails(ArrayList<ArrayList<String>> model) {


        ArrayList<ArrayList<?>> split_feature = new ArrayList<>();
        ArrayList<ArrayList<?>> threshold = new ArrayList<>();

        ArrayList<ArrayList<?>> left_child = new ArrayList<>();
        ArrayList<ArrayList<?>> right_child = new ArrayList<>();


        ArrayList<ArrayList<?>> leaf_value = new ArrayList<>();

        HashMap<String, ArrayList<ArrayList<?>>> modelMap = new HashMap<>();

        for (ArrayList<String> trees : model) {

            for (String tree : trees) {
                if (tree.startsWith("split_feature")) {

                    String[] split = tree.split("=")[1].split(" ");
                    ArrayList<Integer> al = new ArrayList<>();
                    for (String s : split) {
                        al.add(Integer.parseInt(s));
                    }
                    split_feature.add(al);
                }
                if (tree.startsWith("threshold")) {
                    String[] split = tree.split("=")[1].split(" ");
                    ArrayList<Float> al = new ArrayList<>();
                    for (String s : split) {
                        al.add(Float.parseFloat(s));
                    }
                    threshold.add(al);

                }

                if (tree.startsWith("left_child")) {

                    String[] split = tree.split("=")[1].split(" ");
                    ArrayList<Integer> al = new ArrayList<>();
                    for (String s : split) {
                        al.add(Integer.parseInt(s));
                    }
                    left_child.add(al);
                }

                if (tree.startsWith("right_child")) {
                    String[] split = tree.split("=")[1].split(" ");
                    ArrayList<Integer> al = new ArrayList<>();
                    for (String s : split) {
                        al.add(Integer.parseInt(s));
                    }
                    right_child.add(al);
                }
                if (tree.startsWith("leaf_value")) {
                    String[] split = tree.split("=")[1].split(" ");
                    ArrayList<Double> al = new ArrayList<>();
                    for (String s : split) {
                        al.add(Double.parseDouble(s));
                    }
                    leaf_value.add(al);
                }

            }
        }

        modelMap.put("split_feature", split_feature);
        modelMap.put("threshold", threshold);
        modelMap.put("left_child", left_child);
        modelMap.put("right_child", right_child);
        modelMap.put("leaf_value", leaf_value);
        return modelMap;
    }
}
