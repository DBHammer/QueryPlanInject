package org.dbhammer;

import org.dbhammer.bean.CostAndLatencyPair;
import org.dbhammer.bean.QueryPlanInfo;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

            // Generate the new query with join order hints
            // Don't use generatePhysicalOpHint, maybe run exceed times

            // OB
            String obQueryHintByPG = obInstance.generateJoinOrderHint(pgInfo.getJoinOrder());
            String obQueryHintByTiDB = obInstance.generateJoinOrderHint(tidbInfo.getJoinOrder());

            // PG
            String pgQueryHintByOB = pgInstance.generateJoinOrderHint(obInfo.getJoinOrder());
            String pgQueryHintByTiDB = pgInstance.generateJoinOrderHint(tidbInfo.getJoinOrder());

            // TiDB
            String tidbQueryHintByPG = tidbInstance.generateJoinOrderHint(pgInfo.getJoinOrder());
            String tidbQueryHintByOB = tidbInstance.generateJoinOrderHint(obInfo.getJoinOrder());

            // from ... where
            String[] splitedSQLSelect = query.split("select");
            String[] splitedSQLFrom = splitedSQLSelect[1].split("from");
            String[] splitedSQLWhere = splitedSQLFrom[1].split("where");

            // OB
            String obNewQueryByPG = "select /*+ " + obQueryHintByPG + " */ " + splitedSQLSelect[1];
            String obNewQueryByTiDB = "select /*+ " + obQueryHintByTiDB + " */ " + splitedSQLSelect[1];

            // PG
            String pgNewQueryByOB = "select " + splitedSQLFrom[0] + " from " + pgQueryHintByOB + " where "
                    + splitedSQLWhere[1];
            String pgNewQueryByTiDB = "select " + splitedSQLFrom[0] + " from " + pgQueryHintByTiDB + " where "
                    + splitedSQLWhere[1];

            String tidbNewQueryByPG = "select /*+ " + tidbQueryHintByPG + " */ " + splitedSQLSelect[1];
            String tidbNewQueryByOB = "select /*+ " + tidbQueryHintByOB + " */ " + splitedSQLSelect[1];

            // cold run and hot run
            for (int i = 0; i < 2; i++) {
                // OB
                CostAndLatencyPair obOriQueryResult = obInstance.executeQuery(query);
                CostAndLatencyPair obNewQueryResultByPG = obInstance.executeQuery(obNewQueryByPG);
                CostAndLatencyPair obNewQueryResultByTiDB = obInstance.executeQuery(obNewQueryByTiDB);

                // PG
                CostAndLatencyPair pgOriQueryResult = pgInstance.executeQuery(query);
                CostAndLatencyPair pgNewQueryResultByOB = pgInstance.executeQuery(pgNewQueryByOB);
                CostAndLatencyPair pgNewQueryResultByTiDB = pgInstance.executeQuery(pgNewQueryByTiDB);

                // TiDB
                CostAndLatencyPair tidbOriQueryResult = tidbInstance.executeQuery(query);
                CostAndLatencyPair tidbNewQueryResultByPG = tidbInstance.executeQuery(tidbNewQueryByPG);
                CostAndLatencyPair tidbNewQueryResultByOB = tidbInstance.executeQuery(tidbNewQueryByOB);

                if (i == 1) {
                    compare(obOriQueryResult, obNewQueryResultByPG, obNewQueryResultByTiDB, "OB", "PG", "TiDB");
                    compare(pgOriQueryResult, pgNewQueryResultByOB, pgNewQueryResultByTiDB, "PG", "OB", "TiDB");
                    compare(tidbOriQueryResult, tidbNewQueryResultByPG, tidbNewQueryResultByOB, "TiDB", "PG", "OB");
                }
            }
        }
    }

    public static void compare(CostAndLatencyPair oriQueryResult, CostAndLatencyPair newQueryResult1,
            CostAndLatencyPair newQueryResult2, String oridb, String db1, String db2) {
        StringBuilder content = new StringBuilder();
        content.append(oridb + "\t" + db1 + "\t" + db2 + "\t" + oridb + " plan enumerator is correct").append("\n");
        // 计算加速比并打印结果
        if (newQueryResult1.getLatency() < oriQueryResult.getLatency()) {
            double speedup1 = (double) oriQueryResult.getLatency() /
                    newQueryResult1.getLatency();
            writeStringToFile(db1, db2);
            System.out.println(db1 + " Speed-up: " + speedup1);
        }
        if (newQueryResult2.getLatency() < oriQueryResult.getLatency()) {
            double speedup2 = (double) oriQueryResult.getLatency() /
                    newQueryResult2.getLatency();
            System.out.println(db2 + " Speed-up: " + speedup2);
        }
        Boolean falg = false;
        if (oriQueryResult.isPartialOrderSatisfied(newQueryResult1)
                && oriQueryResult.isPartialOrderSatisfied(newQueryResult2)) {
            System.out.println(oriQueryResult.toString());
            System.out.println(newQueryResult1.toString());
            System.out.println(newQueryResult2.toString());
            System.out.println(oridb + " Plan Enumerator is correct");
            falg = true;
        } else {
            System.out.println(oridb + " Plan Enumerator is incorrect");
            // TODO: Compare Q-Error to check Cardinality Estimation or Cost Model
        }
        content.append(
                oriQueryResult.getLatency() + "\t" + newQueryResult1.getLatency() + "\t" + newQueryResult2.getLatency()
                        + "\t" + falg)
                .append("\n");
        writeStringToFile(content.toString(), oridb+".csv");
    }

    public static void writeStringToFile(String content, String filePath) {
        try {
            // 将字符串转换为字节数组
            byte[] bytes = content.getBytes();

            // 获取文件路径
            Path path = Paths.get(filePath);

            // 将字节数组写入文件
            Files.write(path, bytes);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}