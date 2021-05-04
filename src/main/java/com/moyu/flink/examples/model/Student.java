package com.moyu.flink.examples.model;

/**
 * 学生对象
 */
public class Student {
    private Integer id;     // 学生编号
    private String name;    // 学生姓名
    private Double score;   // 学生分数

    public Student() {}

    public Student(Integer id, String name, Double score) {
        this.id = id;
        this.name = name;
        this.score = score;
    }

    @Override
    public String toString() {
        return "Student{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", score=" + score +
                '}';
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }
}
