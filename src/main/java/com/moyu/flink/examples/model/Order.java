package com.moyu.flink.examples.model;

/***
 *      订单数据
 */

public class Order {
    private Integer id;
    private String goodsName;
    private Integer stauts;
    private String createtime;

    public Order() { }

    public Order(Integer id, String goodsName, Integer stauts, String createtime) {
        this.id = id;
        this.goodsName = goodsName;
        this.stauts = stauts;
        this.createtime = createtime;
    }

    @Override
    public String toString() {
        return "Order{" +
                "id=" + id +
                ", goodsName='" + goodsName + '\'' +
                ", stauts=" + stauts +
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

    public Integer getStauts() {
        return stauts;
    }

    public void setStauts(Integer stauts) {
        this.stauts = stauts;
    }

    public String getCreatetime() {
        return createtime;
    }

    public void setCreatetime(String createtime) {
        this.createtime = createtime;
    }
}
