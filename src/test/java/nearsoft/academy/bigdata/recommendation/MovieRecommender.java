package nearsoft.academy.bigdata.recommendation;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class MovieRecommender {

    private final String CSV_FILENAME = "MoviesFormated.csv";
    private final int NUMBER_RECOMMENDATIONS = 3;
    private final String[] REVIEW_INFORMATION = {"review/userId", "product/productId", "review/score"};
    private final String CHARSET_DECODER = "UTF8";
    private UserBasedRecommender recommender;
    private int totalReviews = 0;
    private HashBiMap<String, Integer> usersList;
    private HashBiMap<String, Integer> productsList;

    public MovieRecommender(String dataSource){

        try{
            this.setupAttrs(dataSource);
        }
        catch(Exception e){
            e.printStackTrace();
        }

    }

    private final void setupAttrs(String dataSource){

        try{
            String csvPathFile = FileToCSVFormatFile(dataSource);
            DataModel model = new FileDataModel(new File(csvPathFile));
            UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
            UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1, similarity, model);
            recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);
        }
        catch(Exception e){
            e.printStackTrace();
        }

    }

    /*
        Read the movies data file, create the csv file required, and update: totalReviews, productsList, usersList
    */
    private String FileToCSVFormatFile(String dataSource){

        Writer writer = null;
        this.usersList = HashBiMap.create();
        this.productsList = HashBiMap.create();

        try{

            File source = new File(dataSource);
            File csvFile = new File(source.getParentFile().getAbsolutePath() + "/" + CSV_FILENAME);
            InputStream fileStream = new FileInputStream(dataSource);
            InputStream gzipStream = new GZIPInputStream(fileStream);
            Reader decoder = new InputStreamReader(gzipStream, CHARSET_DECODER);
            BufferedReader buffered = new BufferedReader(decoder);

            if(csvFile.exists()){
                csvFile.delete();
            }

            csvFile.createNewFile();

            writer = new BufferedWriter(new FileWriter(csvFile));
            String currentFileLine;
            Integer userId = null;
            String score = "";
            Integer productId = null;

            while((currentFileLine = buffered.readLine()) != null){

                /* review/userId */
                if(currentFileLine.contains(REVIEW_INFORMATION[0])){

                    this.totalReviews++;

                    String userName = extractValue(currentFileLine);

                    if(!this.usersList.containsKey(userName)){

                        this.usersList.put(userName, this.usersList.size() + 1);

                    }

                    userId = this.usersList.get(userName);

                }
                /* product/productId */
                else if(currentFileLine.contains(REVIEW_INFORMATION[1])){

                    String productName = extractValue(currentFileLine);

                    if(!this.productsList.containsKey(productName)){

                        this.productsList.put(productName, this.productsList.size() + 1);

                    }

                    productId = this.productsList.get(productName);

                }
                /* review/score */
                else if(currentFileLine.contains(REVIEW_INFORMATION[2])){

                    score = extractValue(currentFileLine);

                    writer.append(String.valueOf(userId) + ',' + String.valueOf(productId) + ',' + score + "\n");

                }

            }

        return csvFile.getAbsolutePath();

        }catch(IOException e) {

            e.printStackTrace();
            throw new RuntimeException("Error processing the data source " + dataSource, e);

        }
        finally{
            try{
                if(writer != null){
                    writer.close();
                }
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }

    public int getTotalProducts(){

        return this.productsList.size();

    }

    public int getTotalUsers(){

        return this.usersList.size();

    }

    public int getTotalReviews() {

        return this.totalReviews;

    }

    public List<String> getRecommendationsForUser(String userName) throws TasteException{

        List<RecommendedItem> recommendations = recommender.recommend(this.usersList.get(userName), NUMBER_RECOMMENDATIONS);
        List<String> recommendedMovies = new ArrayList<String>();
        BiMap<Integer, String> productsByName = this.productsList.inverse();

        for(RecommendedItem recommendation : recommendations){

            String movieName = productsByName.get((int)recommendation.getItemID());
            recommendedMovies.add(movieName);

        }

        return recommendedMovies;
    }

    private String extractValue(String str){

        return str.substring(str.indexOf(":") + 2, str.length());

    }

}
