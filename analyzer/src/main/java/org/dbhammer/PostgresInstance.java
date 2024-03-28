package org.dbhammer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.dbhammer.bean.CostAndLatencyPair;
import org.dbhammer.bean.QueryPlanInfo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PostgresInstance implements DBInstance {
    private Connection connection;
    private static final Pattern tablePattern = Pattern.compile("\\b(\\w+)\\.\\w+\\s*=\\s*(\\w+)\\.\\w+");

    @Override
    public Connection getConnection() {
        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection("jdbc:postgresql://49.52.27.20:5452/artemis", "postgres", "wsy");
            System.out.println("Connected to pg");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }

    @Override
    public CostAndLatencyPair executeQuery(String query) {
        double cost = getCostFromQueryPlan(query);
        long startTime = System.currentTimeMillis();
        try (PreparedStatement stmt = connection.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
        } catch (Exception e) {
            e.printStackTrace();
        }
        long endTime = System.currentTimeMillis();
        return new CostAndLatencyPair(cost, (endTime - startTime));
    }

    @Override
    public String getQueryPlan(String query) {
        String explainQuery = "EXPLAIN (FORMAT JSON) " + query;
        StringBuilder jsonResult = new StringBuilder();
        try (PreparedStatement stmt = connection.prepareStatement(explainQuery);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next())
                jsonResult.append(rs.getString(1));
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
            if (rootNode.isArray())
                extractOperations(rootNode.get(0).path("Plan"), queryPlanInfo, new ArrayList<>());
            return queryPlanInfo;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void extractOperations(JsonNode node, QueryPlanInfo queryPlanInfo, List<String> involvedTables) {
        if (node == null) return;
        if (node.has("Plans"))
            for (JsonNode subOp : node.get("Plans"))
                extractOperations(subOp, queryPlanInfo, involvedTables);
        processJoinCondition("Join Filter", node, queryPlanInfo, involvedTables);       // 提取 NLJ 的条件
        processJoinCondition("Hash Cond", node, queryPlanInfo, involvedTables);
        processJoinCondition("Merge Cond", node, queryPlanInfo, involvedTables);
    }

    private static void processJoinCondition(String conditionType, JsonNode node, QueryPlanInfo queryPlanInfo, List<String> involvedTables) {
        if (node.has(conditionType)) {
            String condition = node.path(conditionType).asText();
            Matcher matcher = tablePattern.matcher(condition);
            while (matcher.find()) {
                String table1Name = matcher.group(1);
                if (!involvedTables.contains(table1Name)) {
                    involvedTables.add(table1Name);
                    queryPlanInfo.addJoinOrder(table1Name);
                }
                String table2Name = matcher.group(2);
                if (!involvedTables.contains(table2Name)) {
                    involvedTables.add(table2Name);
                    queryPlanInfo.addJoinOrder(table2Name);
                }
                if (!condition.isEmpty())
                    queryPlanInfo.addJoinOperation(new QueryPlanInfo.JoinOperation(table1Name, table2Name, node.path("Node Type").asText(), 0, 0));
            }
        }
    }

    // TODO
    @Override
    public String generateJoinOrderHint(List<String> joinOrder) {
        if (joinOrder == null || joinOrder.isEmpty())
            return "";
        String joinOrderStr = String.join(" ", joinOrder);
        return String.format(" Leading(%s) ", joinOrderStr);
    }


    // TODO
    @Override
    public String generatePhysicalOpHint(List<QueryPlanInfo.JoinOperation> physicalOp) {
        if (physicalOp == null || physicalOp.isEmpty()) {
            return "";
        }

        StringBuilder hintBuilder = new StringBuilder("");
        for (QueryPlanInfo.JoinOperation operation : physicalOp) {
            switch (operation.getJoinType()) {
                case "Nest Loop":
                    hintBuilder.append(" NestLoop(").append(operation.getTable1()).append(" ").append(operation.getTable2()).append(")");
                    break;
                case "Hash Join":
                    hintBuilder.append(" HashJoin(").append(operation.getTable1()).append(" ").append(operation.getTable2()).append(")");
                    break;
                case "Merge Join":
                    hintBuilder.append(" MergeJoin(").append(operation.getTable1()).append(" ").append(operation.getTable2()).append(")");
                    break;
            }
        }
        return hintBuilder.toString();
    }

    public double getCostFromQueryPlan(String query) {
        double totalCost = 0.0;
        try {
            String jsonPlan = getQueryPlan(query);
            if (jsonPlan == null)
                return 0.0;
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(jsonPlan);
            JsonNode planNode = rootNode.get(0).get("Plan");
            if (planNode != null && planNode.has("Total Cost"))
                totalCost = planNode.get("Total Cost").asDouble();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return totalCost;
    }
}
