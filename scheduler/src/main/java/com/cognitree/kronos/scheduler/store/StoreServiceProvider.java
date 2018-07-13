package com.cognitree.kronos.scheduler.store;

import java.util.HashMap;
import java.util.Map;

public class StoreServiceProvider {

    private static final Map<String, StoreService> STORE_SERVICE_MAP = new HashMap<>();

    public static void registerStoreService(StoreService storeService) {
        STORE_SERVICE_MAP.put(storeService.getName(), storeService);
    }

    public static StoreService getStoreService(String name) {
        return STORE_SERVICE_MAP.get(name);
    }
}
