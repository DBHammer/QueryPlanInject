package org.dbhammer.bean;


import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class CostAndLatencyPair {
    private double cost;
    private double latency;

    public CostAndLatencyPair(double cost, double latency) {
        this.cost = cost;
        this.latency = latency;
    }

    public boolean isPartialOrderSatisfied(CostAndLatencyPair other) {
        return (this.cost <= other.cost && this.latency <= other.latency) || (this.cost >= other.cost && this.latency >= other.latency);
    }
}
