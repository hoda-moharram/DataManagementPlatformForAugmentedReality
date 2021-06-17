package com.mongodb.quickstart;

import com.mongodb.BasicDBObject;

import com.mongodb.client.*;
import org.bson.Document;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static com.mongodb.client.model.Filters.*;



public class Read {

    public static void main(String[] args) {
//        findOneDoc("CreatedOn", "2021.06.14");
//        findListDoc("CreatedOn", "2021.06.14");
//        findListDocsByDate("FileSize", "3", "315");
//        findListDocsBySize("FileSize", 3, 315);

    }

    public static void searchByFileName (Object name){
        findOneDoc("FileName", name);
    }
    public static void searchByFilePath (Object path){
        findOneDoc("FilePath", path);
    }
    public static void searchByCreatedOn (Object date){
        findOneDoc("CreatedOn", date);
    }
    public static void searchByLastUpdated (Object date){
        findOneDoc("LastUpdated", date);
    }
    public static void searchBySize (Object size){
        findOneDoc("FileSize in (KB)", size);
    }

    public static void searchListByFileName (Object name){
        findListDoc("FileName", name);
    }
    public static void searchListByFilePath (Object path){
        findListDoc("FilePath", path);
    }
    public static void searchListByCreatedOn (Object date){
        findListDoc("CreatedOn", date);
    }
    public static void searchListByLastUpdated (Object date){
        findListDoc("LastUpdated", date);
    }
    public static void searchListBySize (Object size){
        findListDoc("FileSize in (KB)", size);
    }

    public static void searchByLastUpdatedRange (Object start, Object end){
        findListDocsByDate("LastUpdated", start, end);
    }
    public static void searchByCreatedOnRange (Object start, Object end){
        findListDocsByDate("CreatedOn", start, end);
    }

    public static void searchBySizeRange (Object start, Object end){
        findListDocsBySize("FileSize in (KB)", start, end);
    }

    private static void findOneDoc(String filter, Object value) {

        try (MongoClient mongoClient = MongoClients.create("mongodb+srv://m220student:m220password@cluster0.fehw5.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")) {
            MongoDatabase sampleTrainingDB = mongoClient.getDatabase("sample_training");
            MongoCollection<Document> filesCollection = sampleTrainingDB.getCollection("Just work");

            if(filter=="CreatedOn"||filter=="LastUpdated"){
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat ("yyyy.MM.dd");
                String date = value.toString();
                Date startDate = simpleDateFormat.parse(date);

                Calendar c = Calendar.getInstance();
                c.setTime(simpleDateFormat.parse(date));
                c.add(Calendar.DATE, 1);  // number of days to add
                String newDate = simpleDateFormat.format(c.getTime());  // dt is now the new date
                Date endDate = simpleDateFormat.parse(newDate);

                //querying
                BasicDBObject query1 = new BasicDBObject(filter, new BasicDBObject("$gte",startDate).append("$lt",endDate));
                Document filesList2 = filesCollection.find(query1).first();

                //printing
                System.out.println("File 1:" + filesList2.toJson());
            }else{
                Document file1 = filesCollection.find(new Document(filter, value)).first();
                System.out.println("File 1 " + file1.toJson());
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
    private static void findListDoc(String filter, Object value) {
        try (MongoClient mongoClient = MongoClients.create("mongodb+srv://m220student:m220password@cluster0.fehw5.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")) {
            MongoDatabase sampleTrainingDB = mongoClient.getDatabase("sample_training");
            MongoCollection<Document> filesCollection = sampleTrainingDB.getCollection("Just work");

            if(filter=="CreatedOn"||filter=="LastUpdated"){
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat ("yyyy.MM.dd");
                String date = value.toString();
                Date startDate = simpleDateFormat.parse(date);
                Calendar c = Calendar.getInstance();
                c.setTime(simpleDateFormat.parse(date));
                c.add(Calendar.DATE, 1);  // number of days to add
                String newDate = simpleDateFormat.format(c.getTime());  // dt is now the new date
                Date endDate = simpleDateFormat.parse(newDate);

                //querying
                BasicDBObject query1 = new BasicDBObject(filter, new BasicDBObject("$gte",startDate).append("$lt",endDate));
                List <Document> filesList2 = filesCollection.find(query1).into(new ArrayList());

                //printing
                System.out.println("File list with an ArrayList:");
                for (Document file : filesList2) {
                    System.out.println(file.toJson());
                }
            } else {
                List<Document> filesList2 = filesCollection.find(eq(filter, value)).into(new ArrayList<>());
                System.out.println("File list with an ArrayList:");
                for (Document file : filesList2) {
                    System.out.println(file.toJson());
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
    private static void findListDocsByDate (String filter, Object from, Object to){
        try (MongoClient mongoClient = MongoClients.create("mongodb+srv://m220student:m220password@cluster0.fehw5.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")) {
            MongoDatabase sampleTrainingDB = mongoClient.getDatabase("sample_training");
            MongoCollection<Document> filesCollection = sampleTrainingDB.getCollection("Just work");

            //parsing the date
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat ("yyyy.MM.dd");
            Date start = simpleDateFormat.parse(from.toString());
            Date end = simpleDateFormat.parse(to.toString());

            //querying
            BasicDBObject query1 = new BasicDBObject(filter, new BasicDBObject("$gte",start).append("$lt",end));
            List<Document> filesList2 = filesCollection.find(query1).into(new ArrayList<>());

            //printing
            System.out.println("File list with an ArrayList:");
            for (Document file : filesList2) {
                System.out.println(file.toJson());
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
    private static void findListDocsBySize (String filter, Object startSize, Object endSize){
        try (MongoClient mongoClient = MongoClients.create("mongodb+srv://m220student:m220password@cluster0.fehw5.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")) {
            MongoDatabase sampleTrainingDB = mongoClient.getDatabase("sample_training");
            MongoCollection<Document> filesCollection = sampleTrainingDB.getCollection("Just work");

            BasicDBObject query1 = new BasicDBObject(filter, new BasicDBObject("$gte", startSize).append("$lt", endSize));
            List<Document> filesList2 = filesCollection.find(query1).into(new ArrayList<>());

            //printing
            System.out.println("File list with an ArrayList:");
            for (Document file : filesList2) {
                System.out.println(file.toJson());
            }
        }
    }

}