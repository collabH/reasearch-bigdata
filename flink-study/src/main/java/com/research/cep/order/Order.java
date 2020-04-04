package com.research.cep.order;

import java.util.Objects;

/**
 * @fileName: Order.java
 * @description: Order.java类说明
 * @author: by echo huang
 * @date: 2020-04-03 18:17
 */
public class Order {
    private String id;
    //订单状态 1:创建订单 2:支付  3:支付成功 4:支付超时
    private Integer state;


    private long orderTime;

    public long getOrderTime() {
        return orderTime;
    }

    public void setOrderTime(long orderTime) {
        this.orderTime = orderTime;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Integer getState() {
        return state;
    }

    public void setState(Integer state) {
        this.state = state;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Order order = (Order) o;
        return orderTime == order.orderTime &&
                id.equals(order.id) &&
                state.equals(order.state);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, state, orderTime);
    }

    @Override
    public String toString() {
        return "Order{" +
                "id='" + id + '\'' +
                ", state=" + state +
                ", orderTime=" + orderTime +
                '}';
    }
}
