package com.vsware.libraries.redisreactive.cache.model;

import java.time.LocalDateTime;
import java.util.StringJoiner;

public class TestTable {

    private Integer id;
    private String name;
    private LocalDateTime insertDate;

    public TestTable() {
    }

    public TestTable(Integer id, String name, LocalDateTime insertDate) {
        this.id = id;
        this.name = name;
        this.insertDate = insertDate;
    }

    public Integer getId() {
        return id;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public LocalDateTime getInsertDate() {
        return insertDate;
    }

    public void setInsertDate(LocalDateTime insertDate) {
        this.insertDate = insertDate;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", TestTable.class.getSimpleName() + "[", "]")
                .add("id=" + id)
                .add("name='" + name + "'")
                .add("insertDate=" + insertDate)
                .toString();
    }
}
