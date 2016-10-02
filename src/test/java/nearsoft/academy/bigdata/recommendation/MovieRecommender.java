package nearsoft.academy.bigdata.recommendation;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

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

public class MovieRecommender{

    private String filepath;
    private HashMap usersMap = new HashMap();
    private HashMap productsMap = new HashMap();
    private int reviews = 0;
    private final String FILEPATH_TO_WRITE = "/home/marco/Documents/Nearsoft/Weeks/03/real/big-data-exercises/MRFormat.csv";

    public MovieRecommender(String filepath){

        this.filepath = filepath;
        this.createCSV(this.filepath);

    }

    public int getTotalReviews(){

        return this.reviews;

    }

    public int getTotalProducts(){

        return this.getProductsMap().size();

    }

    public int getTotalUsers(){

        return this.getUsersMap().size();

    }



    public String getFilepath(){
        return this.filepath;
    }

    public HashMap getUsersMap(){
        return this.usersMap;
    }

    public HashMap getProductsMap(){
        return this.productsMap;
    }

    public void setUsersMap(String key, String value){
        this.usersMap.put(key, value);
    }

    public void setProductsMap(String key, String value){
        this.productsMap.put(key, value);
    }

    public int getReviews(){
        return this.reviews;
    }

    public void incrementReviews(){
        this.reviews++;
    }


    private void createCSV(String filepath){

        String currentLine = null;
        BufferedWriter bufferedWriter = null;

        try{

            InputStream fileStream = new FileInputStream(filepath);
            InputStream gzipStream = new GZIPInputStream(fileStream);
            Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
            BufferedReader buffered = new BufferedReader(decoder);

            File file = new File(FILEPATH_TO_WRITE);

            if(!file.exists()){

                file.createNewFile();

            }

            FileWriter fileWriter = new FileWriter(file);
            bufferedWriter = new BufferedWriter(fileWriter);

            String productID = null;
            String userID = null;
            String score = null;

            while( (currentLine = buffered.readLine()) != null ){

                if(currentLine.contains("review/text:")){ continue; }

                String reviewInfo = null;

                if(currentLine.contains("product/productId:")){

                    this.incrementReviews();

                    reviewInfo = currentLine.split("product/productId:")[1].trim();
                    HashMap mp = this.getProductsMap();

                    if(mp.get(reviewInfo) == null){

                        this.setProductsMap(reviewInfo, Integer.toString(mp.size() + 1));

                    }

                    productID = (String)mp.get(reviewInfo);

                }
                else if(currentLine.contains("review/userId:")){

                    reviewInfo = currentLine.split("review/userId:")[1].trim();
                    HashMap mp = this.getUsersMap();

                    if(mp.get(reviewInfo) == null){

                        this.setUsersMap(reviewInfo, Integer.toString(mp.size() + 1));

                    }
                    userID = (String)mp.get(reviewInfo);

                }
                else if(currentLine.contains("review/score:")){

                    score = currentLine.split("review/score:")[1].trim();

                    bufferedWriter.write(userID + "," + productID + "," + score + "\n");
                }


            }

            System.out.println("REVIEWS: " + this.getTotalReviews());
            System.out.println("PRODUCTS: " + this.getTotalProducts());
            System.out.println("USERS: " + this.getTotalUsers());

        }
        catch(Exception e){
            e.printStackTrace();
        }
        finally{

            try{
                if(bufferedWriter != null){
                    bufferedWriter.close();
                }
            }
            catch(Exception e){
                e.printStackTrace();
            }

        }

    }

    private void processCurrentLine(String currentLine){

        String reviewInfo = null;

        if(currentLine.contains("product/productId:")){

            this.incrementReviews();

            // System.out.println("PRODUCT ID");
            reviewInfo = currentLine.split("product/productId:")[1].trim();
            HashMap mp = this.getProductsMap();

            if(mp.get(reviewInfo) == null){

                this.setProductsMap(reviewInfo, Integer.toString(mp.size() + 1));

            }

        }
        else if(currentLine.contains("review/userId:")){

            reviewInfo = currentLine.split("review/userId:")[1].trim();
            HashMap mp = this.getUsersMap();

            if(mp.get(reviewInfo) == null){

                this.setUsersMap(reviewInfo, Integer.toString(mp.size() + 1));

            }

        }

    }

    private void listHashMap(HashMap hashmap){

        Set set = hashmap.entrySet();
        Iterator i = set.iterator();


        while(i.hasNext()){

            Map.Entry entry = (Map.Entry)i.next();
            // System.out.println("Key: " + entry.getKey());
            // System.out.println("Value: " + entry.getValue());

        }
        System.out.println("Size: " + hashmap.size());

    }

    // public static void main(String[] args) {
    //
    //     // MovieRecommender mv = new MovieRecommender("./smallMovies.txt.tar.gz");
    //     MovieRecommender mv = new MovieRecommender("./movies.txt.gz");
    //     // System.out.println("File path: " + mv.getFilepath());
    //
    // }

}
