package com.cognitree.kronos.scheduler.store;

import com.cognitree.kronos.Service;

import java.util.List;

public interface StoreService<E, T> extends Service {

    void store(E entity);

    List<E> load();

    E load(T identity);

    void update(E entity);

    void delete(T identity);
}
