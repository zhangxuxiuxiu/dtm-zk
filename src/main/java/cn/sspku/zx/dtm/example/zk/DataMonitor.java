package cn.sspku.zx.dtm.example.zk;


/**
 * A simple class that monitors the data and existence of a ZooKeeper
 * node. It uses asynchronous ZooKeeper APIs.
 */
import java.util.Arrays;

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class DataMonitor implements Watcher, StatCallback {

    ZooKeeper zk;

    String znode;

    Watcher chainedWatcher;

    boolean dead;

    DataMonitorListener listener;

    byte prevData[];

    public DataMonitor(ZooKeeper zk, String znode, Watcher chainedWatcher,
            DataMonitorListener listener) {
        this.zk = zk;
        this.znode = znode;
        this.chainedWatcher = chainedWatcher;
        this.listener = listener;
        // Get things started by checking if the node exists. We are going
        // to be completely event driven
        zk.exists(znode, true, this, null);
    }

    /**
     * Other classes use the DataMonitor by implementing this method
     */
    public interface DataMonitorListener {
        /**
         * The existence status of the node has changed.
         * if data is null, the executable will be shut down;
         * else it will be stopped and restarted. 
         * 
         * @param data
         */
        void exists(byte data[]);

        /**
         * The ZooKeeper session is no longer valid.
         * when this is triggered, it's up to the Executor to decide whether or not shut itself down.
         * 
         * @param rc the ZooKeeper reason code
         */
        void closing(int rc);
    }
    
    /** 
     * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
     * 
     * if the session expired, the variable will marked as true, then Executor will shut down.
     * if data changes happens to znode, 
     */
    @Override
    public void process(WatchedEvent event) {

        String path = event.getPath();
        //connection changed
        if (event.getType() == Event.EventType.None) {
            // We are are being told that the state of the
            // connection has changed
            switch (event.getState()) {
            case SyncConnected:
                // In this particular example we don't need to do anything
                // here - watches are automatically re-registered with 
                // server and any watches triggered while the client was 
                // disconnected will be delivered (in order of course)
                break;
            case Expired:
                // It's all over
                dead = true;
                listener.closing(Code.SESSIONEXPIRED.intValue());
                break;
            default:
                    break;
            }
        }
        //data changed
        else {
            if (path != null && path.equals(znode)) {
                // Something has changed on the node, let's find out
                zk.exists(znode, true, this, null);
            }
        }
        if (chainedWatcher != null) {
            chainedWatcher.process(event);
        }
    }

    /** 
     * @see org.apache.zookeeper.AsyncCallback.StatCallback#processResult(int, java.lang.String, java.lang.Object, org.apache.zookeeper.data.Stat)
     * 
     * callback to zookeeper's asynchronous invoking exists method
     */
    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {

        //record current state according to the ZK's event
        boolean exists;
        switch (Code.get(rc)) {
            case OK:
                exists = true;
                break;
            case NONODE:
                exists = false;
                break;
            case SESSIONEXPIRED:
            case NOAUTH:
                dead = true;
                listener.closing(rc);
                return;
            default:
                // Retry errors
                zk.exists(znode, true, this, null);
                return;
        }

        //if the node does exist, the date will be retrieved again
        byte b[] = null;
        if (exists) {
            try {
                b = zk.getData(znode, false, null);
            } catch (KeeperException e) {
                // We don't need to worry about recovering now. The watch
                // callbacks will kick off any exception handling
                e.printStackTrace();
            } catch (InterruptedException e) {
                return;
            }
        }

        //date changed: date got emptied or overwritten
        if ((b == null && b != prevData)
                || (b != null && !Arrays.equals(prevData, b))) {
            listener.exists(b);
            prevData = b;
        }
    }
}

