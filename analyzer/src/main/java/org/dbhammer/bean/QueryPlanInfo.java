package org.dbhammer.bean;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Setter
@Getter
public class QueryPlanInfo {
    private List<String> joinOrder;
    private List<JoinOperation> physicalOp;

    public QueryPlanInfo() {
        joinOrder = new ArrayList<>();
        physicalOp = new ArrayList<>();
    }


    public void addJoinOperation(JoinOperation operation) {
        physicalOp.add(operation);
    }

    public void addJoinOrder(String tableName) {
        if (!joinOrder.contains(tableName)) {
            joinOrder.add(tableName);
        }
    }



    @Setter
    @Getter
    public static class JoinOperation {
        private String table1;
        private String table2;
        private String joinType;
        private int estimatedCardinality;
        private int trueCardinality;

        public JoinOperation(String table1, String table2, String joinType, int estimatedCardinality, int trueCardinality) {
            this.table1 = table1;
            this.table2 = table2;
            this.joinType = joinType;
            this.estimatedCardinality = estimatedCardinality;
            this.trueCardinality = trueCardinality;
        }


    }
}
