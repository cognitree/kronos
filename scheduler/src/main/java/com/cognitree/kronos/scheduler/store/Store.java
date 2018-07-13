package com.cognitree.kronos.scheduler.store;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

public interface Store<E, T> {

    /**
     * called during initialization phase to initialize the task store using {@link TaskStoreConfig#config}. Any property
     * required by the store to instantiate itself should be part of {@link TaskStoreConfig#config}.
     *
     * @param storeConfig configuration used to initialize the store.
     * @throws Exception
     */
    void init(ObjectNode storeConfig) throws Exception;

    void store(E entity);

    List<E> load();

    E load(T identity);

    void update(E entity);

    void delete(T identity);

    void stop();
}
