package io.crate.qa.kafka;

import java.util.Objects;

public class Metrics {

    final Integer id;

    final Integer x;

    public Metrics(Integer id, Integer x) {
        this.id = id;
        this.x = x;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Metrics metrics = (Metrics) o;
        return Objects.equals(id, metrics.id) &&
                Objects.equals(x, metrics.x);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, x);
    }
}
