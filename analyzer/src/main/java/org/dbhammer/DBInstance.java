package org.dbhammer;

import org.dbhammer.bean.CostAndLatencyPair;
import org.dbhammer.bean.QueryPlanInfo;

import java.sql.Connection;
import java.util.List;

public interface DBInstance {

    Connection getConnection();

    CostAndLatencyPair executeQuery(String query);

    String getQueryPlan(String query);

    QueryPlanInfo extractQueryPlan(String obPlan);

    String generateJoinOrderHint(List<String> joinOrder);

    String generatePhysicalOpHint(List<QueryPlanInfo.JoinOperation> physicalOp);

}
