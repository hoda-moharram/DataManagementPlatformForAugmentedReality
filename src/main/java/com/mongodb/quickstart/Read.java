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


public class Read extends CRUD {

    /*
    This Read class shows information which the user searches for.
    Multiple filters are included such as searching by name, a substring of the name or filepath, filepath, description, etc.
    User can search for one specific document or a list of documents that satisfy the condition.
    If the record a user is searching for does not exist, a log entry is added in the log file stating that.
     */

    public static void main(String[] args) throws ParseException {
        findListDoc("FilePath", "ze");
//        findListDoc("FileName", "Bottle.step");
//        findListDocsByDate("FileSize", "3", "315");
//        findListDocsBySize("FileSize", 3, 315);
//        searchByFilePath("/Users/hodamoharram/Desktop/Bottle.step");
//        searchListByCreatedOn("2021.07.15");
//        searchByFileName("editing.pages");
//        searchListByCreatedOn("2021.07.15");

    }
    /*
        This method enables searching by name or a substring of it, retrieving only the first document that meet the condition.
    */
    public static void searchByFileName(Object name) throws ParseException {
        findOneDoc("FileName", name);
    }
    /*
        This method enables searching by file path or a substring of it, retrieving only the first document that meet the condition.
     */
    public static void searchByFilePath(Object path) throws ParseException {
        findOneDoc("FilePath", path);
    }
    /*
       This method enables searching by the date a document was created on, retrieving only the first document that meet the condition.
    */
    public static void searchByCreatedOn(Object date) throws ParseException {
        findOneDoc("CreatedOn", date);
    }
    /*
         This method enables searching by the date on which a document was last updated, retrieving only the first document that meet the condition.
    */
    public static void searchByLastUpdated(Object date) throws ParseException {
        findOneDoc("LastUpdated", date);
    }
    /*
       This method enables searching by the size of a document, retrieving only the first document that meet the condition.
    */
    public static void searchBySize(Object size) throws ParseException {
        findOneDoc("FileSize in (KB)", size);
    }
    /*
        This method enables searching by name or a substring of it, retrieving all files that meet the condition
    */
    public static void searchListByFileName(Object name) throws ParseException {
        findListDoc("FileName", name);
    }
    /*
        This method enables searching by file path or a substring of it, retrieving all files that meet the condition
    */
    public static void searchListByFilePath(Object path) throws ParseException {
        findListDoc("FilePath", path);
    }
    /*
       This method enables searching by the date a document was created on, retrieving all files that meet the condition.
    */
    public static void searchListByCreatedOn(Object date) throws ParseException {
        findListDoc("CreatedOn", date);
    }
    /*
       This method enables searching by the date on which a document was last updated, retrieving all files that meet the condition.
    */
    public static void searchListByLastUpdated(Object date) throws ParseException {
        findListDoc("LastUpdated", date);
    }
    /*
       This method enables searching by the size of a document , retrieving all files that meet the condition.
    */
    public static void searchListBySize(Object size) throws ParseException {
        findListDoc("FileSize in (KB)", size);
    }
    /*
       This method enables searching for a document last updated between 2 dates, retrieving all files that meet the condition.
    */
    public static void searchByLastUpdatedRange(Object start, Object end) throws ParseException {
        findListDocsByDate("LastUpdated", start, end);
    }
    /*
       This method enables searching for a document that was created between 2 dates, retrieving all files that meet the condition.
    */
    public static void searchByCreatedOnRange(Object start, Object end) throws ParseException {
        findListDocsByDate("CreatedOn", start, end);
    }
    /*
       This method enables searching for a document with a size range, retrieving all files that meet the condition.
    */
    public static void searchBySizeRange(Object start, Object end) {
        findListDocsBySize("FileSize in (KB)", start, end);
    }

    /*
    This method that fetches the first document that satisfy the condition
     */
    private static void findOneDoc(String filter, Object value) throws ParseException {
        if (filter == "CreatedOn" || filter == "LastUpdated") {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy.MM.dd");
            String date = value.toString();
            Date startDate = simpleDateFormat.parse(date);

            Calendar c = Calendar.getInstance();
            c.setTime(simpleDateFormat.parse(date));
            c.add(Calendar.DATE, 1);  // number of days to add
            String newDate = simpleDateFormat.format(c.getTime());  // dt is now the new date
            Date endDate = simpleDateFormat.parse(newDate);

            //querying
            BasicDBObject query1 = new BasicDBObject(filter, new BasicDBObject("$gte", startDate).append("$lt", endDate));
            Document d = getFilesCollection().find(query1).first();
            if (!d.isEmpty()){
                //updateReads
                updateReads("FilePath", d.get("FilePath"));

                //update stats
                updateStatistics(d);


                //printing
                System.out.println("File 1:" + d.toJson());
            }
            else{
                out.println(filter + ": " + value + " doesn't exist." + new Date());
            }

        } else {
            Document file1 = getFilesCollection().find(regex(filter, (String) value)).first();
            if (!file1.isEmpty()){
                System.out.println("File 1 " + file1.toJson());
                updateReads("FilePath", file1.get("FilePath"));
                updateStatistics(file1);
            }
            else{
                out.println(filter + ": " + value + " doesn't exist." + new Date());
            }


        }
        out.flush();
    }

    /*
        This method that fetches all documents that satisfy the condition
     */
    private static void findListDoc(String filter, Object value) throws ParseException {


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
                List <Document> filesList2 = getFilesCollection().find(query1).into(new ArrayList());

                if (!filesList2.isEmpty()){
                    //update reads
                    for (Document d: filesList2){
                        updateManyReads("FilePath", d.get("FilePath"));
                        updateStatistics(d);
                    }
                    System.out.println("File list with an ArrayList:");
                    for (Document file : filesList2) {
                        System.out.println(file.toJson());
                    }
                }
                else{
                    out.println(filter + ": " + value + " doesn't exist." + new Date());
                }


            } else {
                List<Document> filesList2 = getFilesCollection().find(regex(filter, (String) value)).into(new ArrayList<>());
                if (!filesList2.isEmpty()){
                    //update reads
                    for (Document d: filesList2){
                        updateManyReads("FilePath", d.get("FilePath"));
                        updateStatistics(d);
                    }
                    System.out.println("File list with an ArrayList:");
                    for (Document file : filesList2) {
                        System.out.println(file.toJson());
                    }
                }
                else{
                    out.println(filter + ": " + value + " doesn't exist." + new Date());
                }

            }
        out.flush();

    }

    private static void findListDocsByDate (String filter, Object from, Object to) throws ParseException {


            //parsing the date
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat ("yyyy.MM.dd");
            Date start = simpleDateFormat.parse(from.toString());
            Date end = simpleDateFormat.parse(to.toString());

            //querying
            BasicDBObject query1 = new BasicDBObject(filter, new BasicDBObject("$gte",start).append("$lt",end));
            List<Document> filesList2 = getFilesCollection().find(query1).into(new ArrayList<>());

        if (!filesList2.isEmpty()){
            //update reads
            for (Document d: filesList2){
                updateManyReads("FilePath", d.get("FilePath"));
                updateStatistics(d);
            }
            System.out.println("File list with an ArrayList:");
            for (Document file : filesList2) {
                System.out.println(file.toJson());
            }
        }
        else{
            out.println(filter + ": " + from + " " + to + " doesn't exist." + new Date());
        }

    out.flush();
    }

    private static void findListDocsBySize (String filter, Object startSize, Object endSize){

            BasicDBObject query1 = new BasicDBObject(filter, new BasicDBObject("$gte", startSize).append("$lt", endSize));
            List<Document> filesList2 = getFilesCollection().find(query1).into(new ArrayList<>());

        if (!filesList2.isEmpty()){
            //update reads
            for (Document d: filesList2){
                updateManyReads("FilePath", d.get("FilePath"));
                updateStatistics(d);
            }
            System.out.println("File list with an ArrayList:");
            for (Document file : filesList2) {
                System.out.println(file.toJson());
            }
        }
        else{
            out.println(filter + ": " + startSize + " " + endSize  + " doesn't exist." + new Date());
        }
        out.flush();

    }



    private static void updateStatistics(Document d){

            Double oldFileSize = (Double) d.get("FileSize in (KB)");
            String fp =(String) d.get("FilePath");
            File f = new File (fp);
            Double newFileSize = getFileSizeKiloBytes(f);
            Double diff = newFileSize-oldFileSize;
        Date lastUpdated = new Date();


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
            Bson update4 = set("FilesCollection lastUpdated",lastUpdated );
            Bson updateOperations = combine( update1, update2, update3, update4);
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