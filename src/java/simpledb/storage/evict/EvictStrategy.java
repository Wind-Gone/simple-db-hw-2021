package simpledb.storage.evict;

import simpledb.storage.Page;
import simpledb.storage.PageId;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Hu Zirui
 * @version 1.0.0
 * @ClassName EvictStrategy.java
 * @Description TODO
 * @createTime 2022年05月07日 17:06:00
 */
public interface EvictStrategy {

    PageId getEvictPageId(ConcurrentHashMap<PageId, Page> pages);                    // return the pageid which is to be evicted

}
