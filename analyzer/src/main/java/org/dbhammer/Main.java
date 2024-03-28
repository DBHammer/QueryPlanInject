package org.dbhammer;

import org.dbhammer.bean.CostAndLatencyPair;
import org.dbhammer.bean.QueryPlanInfo;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {
    private static final String SQL_FILE_DIRECTORY = "/home/jw/QueryPlanInject/analyzer/src/main/java/org/dbhammer/artemis_sql/";
    private static final String QErrorThreshold = "10";

    private static List<String> loadAllQueries() {
        try {
            List<String> queries = new ArrayList<>();
            File[] files = new File(SQL_FILE_DIRECTORY).listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isFile() && file.getName().endsWith(".sql")) {
                        String query = new String(Files.readAllBytes(file.toPath()));
                        queries.add(query);
                    }
                }
            }
            return queries;
        } catch (IOException e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    public static void regexExampleForPGINJ() {
        String input = "(primarykey = table_3.fk_3)";
        String regex = "=\\s*(table_\\d+)";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(input);
        if (matcher.find()) {
            System.out.println(matcher.group(1));
        } else {
            System.out.println("No match found");
        }
    }


    public static void main(String[] args) {
        DBInstance obInstance = new OceanBaseInstance();
        DBInstance tidbInstance = new TiDBInstance();
        DBInstance pgInstance = new PostgresInstance();
        List<String> allQueries = loadAllQueries();
        obInstance.getConnection();
        tidbInstance.getConnection();
        pgInstance.getConnection();
        for (String query : allQueries) {
            // Get the query plan for each database
            String obPlan = obInstance.getQueryPlan(query);
            String tidbPlan = tidbInstance.getQueryPlan(query);
            String pgPlan = pgInstance.getQueryPlan(query);

            // Extract the query plan information
            QueryPlanInfo obInfo = obInstance.extractQueryPlan(obPlan);
            QueryPlanInfo tidbInfo = tidbInstance.extractQueryPlan(tidbPlan);
            QueryPlanInfo pgInfo = pgInstance.extractQueryPlan(pgPlan);

            System.out.println(obInfo.getJoinOrder());
            System.out.println(pgInfo.getJoinOrder());
            System.out.println(tidbInfo.getJoinOrder());



            // Generate the new query with join order hints
            String queryHintByPG = obInstance.generateJoinOrderHint(pgInfo.getJoinOrder()) + obInstance.generatePhysicalOpHint(pgInfo.getPhysicalOp());
            String queryHintByTiDB = obInstance.generateJoinOrderHint(tidbInfo.getJoinOrder()) + obInstance.generatePhysicalOpHint(tidbInfo.getPhysicalOp());
            String[] splitSQL = query.split("select");
            String newQueryByPG = "select /*+ query_timeout(40000000) " + queryHintByPG + " */ " + splitSQL[1];
            String newQueryByTiDB = "select /*+ query_timeout(40000000) " + queryHintByTiDB + " */ " + splitSQL[1];
            System.out.println("New Query with Join Order Hints by PG: " + newQueryByPG);
            System.out.println("New Query with Join Order Hints by TiDB: " + newQueryByTiDB);
            CostAndLatencyPair oriQueryResult = obInstance.executeQuery(query);
            CostAndLatencyPair newQueryResultByPG = obInstance.executeQuery(newQueryByPG);
            CostAndLatencyPair newQueryResultByTiDB = obInstance.executeQuery(newQueryByTiDB);
            // 计算加速比并打印结果
            if (newQueryResultByTiDB.getLatency() < oriQueryResult.getLatency()) {
                double speedupTiDB = (double) oriQueryResult.getLatency() / newQueryResultByTiDB.getLatency();
                System.out.println("TiDB Speed-up: " + speedupTiDB);
            }
            if (newQueryResultByPG.getLatency() < oriQueryResult.getLatency()) {
                double speedupPG = (double) oriQueryResult.getLatency() / newQueryResultByPG.getLatency();
                System.out.println("PG Speed-up: " + speedupPG);
            }
            if (oriQueryResult.isPartialOrderSatisfied(newQueryResultByPG) && oriQueryResult.isPartialOrderSatisfied(newQueryResultByTiDB)) {
                System.out.println("Plan Enumerator is correct");
            } else {
                System.out.println("Plan Enumerator is incorrect");
                // TODO: Compare Q-Error to check Cardinality Estimation or Cost Model
            }
        }
    }
}