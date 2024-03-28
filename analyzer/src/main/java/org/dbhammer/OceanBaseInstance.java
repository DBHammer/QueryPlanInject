package org.dbhammer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.dbhammer.bean.CostAndLatencyPair;
import org.dbhammer.bean.QueryPlanInfo;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class OceanBaseInstance implements DBInstance {
    private Connection connection;

    @Override
    public Connection getConnection() {
        try {
            connection = DriverManager.getConnection("jdbc:mysql://49.52.27.33:2881/test", "root", "123456");
            System.out.println("Connected to the database");
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

    @Override
    public String getQueryPlan(String query) {
        String explainQuery = "EXPLAIN FORMAT=JSON " + query;
        StringBuilder jsonResult = new StringBuilder();
        try (PreparedStatement stmt = connection.prepareStatement(explainQuery);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                jsonResult.append(rs.getString(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.enable(SerializationFeature.INDENT_OUTPUT);
            JsonNode jsonNode = mapper.readTree(jsonResult.toString());
            return mapper.writeValueAsString(jsonNode);
        } catch (Exception e) {
            e.printStackTrace();
            return jsonResult.toString();
        }
    }

    @Override
    public QueryPlanInfo extractQueryPlan(String jsonPlan) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode rootNode = mapper.readTree(jsonPlan);
            QueryPlanInfo queryPlanInfo = new QueryPlanInfo();
            extractOperations(rootNode, queryPlanInfo);
            return queryPlanInfo;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private static void extractOperations(JsonNode node, QueryPlanInfo queryPlanInfo) {
        if (node == null) return;
        List<String> tables = new ArrayList<>();
        extractTableNames(node, tables, queryPlanInfo);
        if (node.has("OPERATOR") && node.get("OPERATOR").asText().toLowerCase().contains("JOIN") && tables.size() >= 2) {
            String operator = node.get("OPERATOR").asText();
            int estRows = node.get("EST.ROWS").asInt();
            String table1 = tables.get(tables.size() - 2);
            String table2 = tables.get(tables.size() - 1);
            queryPlanInfo.addJoinOperation(new QueryPlanInfo.JoinOperation(table1, table2, operator, estRows, estRows));
            queryPlanInfo.addJoinOrder(table1);
            queryPlanInfo.addJoinOrder(table2);
        }
    }

    private static void extractTableNames(JsonNode node, List<String> tables, QueryPlanInfo queryPlanInfo) {
        if (node.has("CHILD_1"))
            extractTableNames(node.get("CHILD_1"), tables, queryPlanInfo);
        if (node.has("CHILD_2"))
            extractTableNames(node.get("CHILD_2"), tables, queryPlanInfo);
        if (node.has("NAME") && !node.get("NAME").asText().isEmpty())
            tables.add(node.get("NAME").asText());
        if (node.has("OPERATOR") && node.get("OPERATOR").asText().contains("JOIN") && tables.size() >= 2) {
            String operator = node.get("OPERATOR").asText();
            int estRows = node.get("EST.ROWS").asInt();
            String table1 = tables.get(tables.size() - 2);
            String table2 = tables.get(tables.size() - 1);
            queryPlanInfo.addJoinOperation(new QueryPlanInfo.JoinOperation(table1, table2, operator, estRows, 0));
            queryPlanInfo.addJoinOrder(table1);
            queryPlanInfo.addJoinOrder(table2);
        }
    }


    @Override
    public String generateJoinOrderHint(List<String> joinOrder) {
        if (joinOrder == null || joinOrder.isEmpty()) {
            return "";
        }
        String joinOrderStr = String.join(",", joinOrder);
        return " leading(" + joinOrderStr + ") ";
    }


    @Override
    public String generatePhysicalOpHint(List<QueryPlanInfo.JoinOperation> physicalOp) {
        StringBuilder hintBuilder = new StringBuilder();
        for (QueryPlanInfo.JoinOperation operation : physicalOp) {
            switch (operation.getJoinType().toLowerCase()) {
                case "mergejoin":       // tidb
                case "merge join":      // pg
                    hintBuilder.append(generateMergeHint(operation.getTable2()));
                    break;
                case "indexjoin":
                case "nested loop":
                    hintBuilder.append(generateNlHint(operation.getTable2()));
                    break;
                case "hashjoin":
                case "hash join":
                    hintBuilder.append(generateHashHint(operation.getTable2()));
                    break;
                default:
                    break;
            }
        }
        return hintBuilder.toString();
    }

    private String generateMergeHint(String tableName) {
        return " use_merge(" + tableName + ") ";
    }

    private String generateNlHint(String tableName) {
        return " use_nl(" + tableName + ") ";
    }

    private String generateHashHint(String tableName) {
        return " use_hash(" + tableName + ") ";
    }

    private double getCostFromQueryPlan(String query) {
        double cost = 0;
        String jsonPlan = getQueryPlan(query);
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(jsonPlan);
            if (rootNode.has("EST.TIME(us)")) {
                cost = rootNode.get("EST.TIME(us)").asDouble();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return cost;
    }

}


