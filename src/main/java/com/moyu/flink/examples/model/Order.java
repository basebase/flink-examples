package com.moyu.flink.examples.model;

/***
 *      订单数据
 */

public class Order {
    private Integer id;
    private String goodsName;
    private Integer amount;
    private String createtime;

    public Order() { }

    public Order(Integer id, String goodsName, Integer amount, String createtime) {
        this.id = id;
        this.goodsName = goodsName;
        this.amount = amount;
        this.createtime = createtime;
    }

    @Override
    public String toString() {
        return "Order{" +
                "id=" + id +
                ", goodsName='" + goodsName + '\'' +
                ", amount=" + amount +
                ", createtime='" + createtime + '\'' +
                '}';
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getGoodsName() {
        return goodsName;
    }

    public void setGoodsName(String goodsName) {
        this.goodsName = goodsName;
    }

    public Integer getAmount() {
        return amount;
    }

    public void setAmount(Integer amount) {
        this.amount = amount;
    }

    public String getCreatetime() {
        return createtime;
    }

    public void setCreatetime(String createtime) {
        this.createtime = createtime;
    }
}
