package simpledb.storage.evict;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.storage.Page;
import simpledb.storage.PageId;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Hu Zirui
 * @version 1.0.0
 * @ClassName FIFO.java
 * @Description TODO
 * @createTime 2022年05月07日 17:17:00
 */
public class LRU implements EvictStrategy {

    @Override
    public PageId getEvictPageId(ConcurrentHashMap<PageId, Page> pages) throws DbException {
        Page exictPage = null;
        boolean isAllDirty = true;
        for (Page page : pages.values()) {
            if (page.isDirty() != null) {                           // flush dirty page to disk
                Database.getBufferPool().flushPage(page.getId());
            }
            if (exictPage == null || page.getLastAccessedTime() < exictPage.getLastAccessedTime()) {        // find satisfying page
                exictPage = page;
                isAllDirty = false;
            }
        }
        if (isAllDirty) {
            throw new DbException("all dirty page in bufferpool");
        }
        return exictPage.getId();
    }
}
