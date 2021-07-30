package com.guanyq.study.TableAPIAndSQL.TableAPI.Aggregations;

/**
 * 新增
 *
 * @author guanyq
 * @date 2021/2/18
 */

public class Order {
    public String orderId;
    public Integer userId;
    public Integer money;
    public Long createTime;

    public Order() {
    }

    public Order(String orderId, Integer userId, Integer money, Long createTime) {
        this.orderId = orderId;
        this.userId = userId;
        this.money = money;
        this.createTime = createTime;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public Integer getMoney() {
        return money;
    }

    public void setMoney(Integer money) {
        this.money = money;
    }

    public Long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", userId=" + userId +
                ", money=" + money +
                ", createTime=" + createTime +
                '}';
    }

}
