package com.mongodb.quickstart;

import com.mongodb.BasicDBObject;

import com.mongodb.client.*;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;


public class Read extends CRUD{

    public static void main(String[] args) {
//        findOneDoc("CreatedOn", "2021.06.14");
//        findListDoc("CreatedOn", "2021.06.14");
//        findListDocsByDate("FileSize", "3", "315");
//        findListDocsBySize("FileSize", 3, 315);
        searchByFilePath("/Users/hodamoharram/Desktop/Bottle.step");

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

        try(MongoClient mongoClient = MongoClients.create("mongodb+srv://m220student:m220password@cluster0.fehw5.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")) {
            MongoDatabase sampleTrainingDB = mongoClient.getDatabase("sample_training");
            MongoCollection<Document> filesCollection = sampleTrainingDB.getCollection("ARObjectsDatabase");

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
                Document d = filesCollection.find(query1).first();


                //updateReads
                updateReads(filter, value);

                //update stats
                updateStatistics(d);


                //printing
                System.out.println("File 1:" + d.toJson());
            }else{
                Document file1 = filesCollection.find(new Document(filter, value)).first();
                System.out.println("File 1 " + file1.toJson());
                updateReads(filter, value);
                updateStatistics(file1);


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

                //updateReads
                updateManyReads(filter, value);

                //update stats
                for(Document d: filesList2){
                    updateStatistics(d);
                }

                //printing
                System.out.println("File list with an ArrayList:");
                for (Document file : filesList2) {
                    System.out.println(file.toJson());
                }
            } else {
                List<Document> filesList2 = filesCollection.find(eq(filter, value)).into(new ArrayList<>());

                for(Document d: filesList2){
                    updateStatistics(d);
                }

                updateManyReads(filter, value);

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

            //update Reads
            updateManyReads(filter, query1);



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

            //update Reads
            updateManyReads(filter, query1);

            //update stats
            for(Document d: filesList2){
                updateStatistics(d);
            }

            //printing
            System.out.println("File list with an ArrayList:");
            for (Document file : filesList2) {
                System.out.println(file.toJson());
            }
        }
    }

    private static void updateStatistics(Document d){

            Double oldFileSize = (Double) d.get("FileSize in (KB)");
            String fp =(String) d.get("FilePath");
            File f = new File (fp);
            Double newFileSize = getFileSizeKiloBytes(f);
            Double diff = newFileSize-oldFileSize;

            MongoCollection<Document> stats = getStatsCollection();

            Bson filter1 = eq("Name","Statistics Document");
            Document statsDoc = stats.find(filter1).first();

            Bson filter2 = eq("FilePath", fp);
            getFilesCollection().updateOne(filter1, set("FileSize in (KB)", newFileSize));

            int filesCount = (int) statsDoc.get("Files Count") + 1;
            Double totalFileSizes = (Double) statsDoc.get("Total Files Storage Space in KB") + diff;
            int totalReadsCount = (int) statsDoc.get("CountTotalReads") + 1;
            Double avgFileSize = totalFileSizes / filesCount;


            Bson update1 = set("Total Files Storage Space in KB", totalFileSizes);
            Bson update2 = set("CountTotalReads", totalReadsCount);
            Bson update3 = set("Average File Size", avgFileSize);
            Bson updateOperations = combine( update1, update2, update3);
            stats.findOneAndUpdate(filter1, updateOperations);


    }

    private static void updateReads(String filter, Object value ){
        Bson filter1 = eq(filter, value);
        Bson updateOperation = inc("CountReads", 1);
        getFilesCollection().updateOne(filter1, updateOperation);
    }
    private static void updateManyReads(String filter, Object value ){
        Bson filter1 = eq(filter, value);
        Bson updateOperation = inc("CountReads", 1);
        getFilesCollection().updateMany(filter1, updateOperation);
    }


}