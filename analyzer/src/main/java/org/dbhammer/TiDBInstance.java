package org.dbhammer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.dbhammer.bean.CostAndLatencyPair;
import org.dbhammer.bean.QueryPlanInfo;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class TiDBInstance implements DBInstance {
    private Connection connection;

    @Override
    public Connection getConnection() {
        try {
            connection = DriverManager.getConnection("jdbc:mysql://49.52.27.20:4000/artemis", "root", "");
            System.out.println("Connected to tidb");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }


    @Override
    public CostAndLatencyPair executeQuery(String query) {
        double cost = getCostFromQueryPlan(query);
        long startTime = System.currentTimeMillis();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
        long endTime = System.currentTimeMillis();
        double latency = endTime - startTime;
        return new CostAndLatencyPair(cost, latency);
    }

    // TiDB cannot acquire estimated cost from the query plan
    private double getCostFromQueryPlan(String query) {
        return 0;
    }


    @Override
    public String getQueryPlan(String query) {
        String explainQuery = "EXPLAIN format=tidb_json " + query;
        StringBuilder plan = new StringBuilder();
        try (PreparedStatement stmt = connection.prepareStatement(explainQuery);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next())
                plan.append(rs.getString(1));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return plan.toString();

    }

    @Override
    public QueryPlanInfo extractQueryPlan(String jsonPlan) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode rootNode = mapper.readTree(jsonPlan);
            QueryPlanInfo queryPlanInfo = new QueryPlanInfo();
            if (rootNode.isArray())
                extractOperations(rootNode.get(0), queryPlanInfo, new ArrayList<>());
            return queryPlanInfo;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void extractOperations(JsonNode node, QueryPlanInfo queryPlanInfo, List<String> involvedTables) {
        if (node == null) return;

        // 查找并提取accessObject字段以获取表名
        if (node.has("accessObject")) {
            String accessObject = node.get("accessObject").asText();
            if (accessObject.startsWith("table:")) {
                String tableName = accessObject.substring(6);
                if (!involvedTables.contains(tableName)) {
                    involvedTables.add(tableName);
                }
            }
        }

        // 递归遍历subOperators
        if (node.has("subOperators")) {
            for (JsonNode subOp : node.get("subOperators")) {
                extractOperations(subOp, queryPlanInfo, involvedTables);
            }
        }

        // 当前节点为Join操作时，提取并记录信息
        if (node.has("id") && node.get("id").asText().toLowerCase().contains("join")) {
            double estRows = node.has("estRows") ? node.get("estRows").asDouble() : 0.0;
            double trueRows = 0;        // TODO: 从TiDB的查询计划中提取trueRows
            if (involvedTables.size() >= 2) {
                String table1 = involvedTables.get(involvedTables.size() - 2);
                String table2 = involvedTables.get(involvedTables.size() - 1);
                int joinIndex = node.get("id").asText().indexOf("Join");
                String joinType = node.get("id").asText().substring(0, joinIndex + 4);
                queryPlanInfo.addJoinOperation(new QueryPlanInfo.JoinOperation(table1, table2, joinType, (int) estRows, (int) trueRows));
                queryPlanInfo.addJoinOrder(table1);
                queryPlanInfo.addJoinOrder(table2);
            }
        }
    }

    @Override
    public String generateJoinOrderHint(List<String> joinOrder) {
        if (joinOrder == null || joinOrder.isEmpty())
            return "";
        String joinOrderStr = String.join(", ", joinOrder);
        return String.format(" LEADING(%s) ", joinOrderStr);
    }

    // TODO UPDATE
    @Override
    public String generatePhysicalOpHint(List<QueryPlanInfo.JoinOperation> physicalOp) {
        StringBuilder hintBuilder = new StringBuilder("");
        for (QueryPlanInfo.JoinOperation op : physicalOp) {
            switch (op.getJoinType()) {
                case "MERGE_JOIN":
                    hintBuilder.append(" MERGE_JOIN(").append(String.join(", ", op.getTable1(), op.getTable2())).append(")");
                    break;
                case "INL_JOIN":
                    hintBuilder.append(" INL_JOIN(").append(String.join(", ", op.getTable1(), op.getTable2())).append(")");
                    break;
                case "HASH_JOIN":
                    hintBuilder.append(" HASH_JOIN(").append(String.join(", ", op.getTable1(), op.getTable2())).append(")");
                    break;
            }
        }
        return hintBuilder.toString();
    }

}
