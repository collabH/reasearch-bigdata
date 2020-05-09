package com.research.cep;

import java.util.Objects;

/**
 * @fileName: Event.java
 * @description: Event.java类说明
 * @author: by echo huang
 * @date: 2020-04-03 10:50
 */
public class Event {
    private String name;
    private String state;
    private Integer sign;

    public Integer getSign() {
        return sign;
    }

    public void setSign(Integer sign) {
        this.sign = sign;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    @Override
    public String toString() {
        return "Event{" +
                "name='" + name + '\'' +
                ", state='" + state + '\'' +
                ", sign=" + sign +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return name.equals(event.name) &&
                state.equals(event.state) &&
                sign.equals(event.sign);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, state, sign);
    }
}
