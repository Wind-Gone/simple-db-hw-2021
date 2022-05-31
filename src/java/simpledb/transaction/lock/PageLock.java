package simpledb.transaction.lock;


/**
 * @author Hu Zirui
 * @version 1.0.0
 * @ClassName PageLock.java
 * @Description TODO
 * @createTime 2022年05月31日 11:30:00
 */

public class PageLock {
    public LockType lockType;

    public PageLock(LockType lockType) {
        this.lockType = lockType;
    }

    @Override
    public String toString() {
        return "PageLock{" +
                "lockType=" + lockType +
                '}';
    }

    public enum LockType {
        EXCLUSIVE, SAHRE
    }
}
