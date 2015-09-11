package cn.sspku.zx.dtm.example.zk.twopc;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.sspku.zx.dtm.example.zk.twopc.XMessage.CoordinatorOrder;

/**
 * Following is general steps involved in distributed transaction, DTCoordinator acts as the coordinator of distributed transaction.
 * 
 *     Functionality: 
 *          1>create a unique transaction directory;
 *          2>notify all participants to start a certain transaction;
 *          3>watch all participants' votes and decide to abort or commit the transaction;
 *          4>watch all participants' final states and decide to end the transaction or request hum intervention;
 * 
 * 
 *      Firstly,coordinator begins a distributed transaction by making a new  directory with a unique transaction id 
 * where all participants will write down their votes and states in the two-phrase commit;
 * 
 *      Secondly, all participants register themselves into the current transaction to the coordinator so that the coordinator would
 * be aware of;
 * 
 *      Thirdly,the coordinator propagate the distributed transaction to all participants;
 *      
 *      Fourthly,every participant will do the PREPARE phrase in 2pc and create its own corresponding node with its vote which is 
 * 'PREPARED' or 'ABORTED' in the transaction directory created by step one;
 * 
 *      Fifthly, on detecting that a 'ABORTED' is written or a participant node is removed, coordinator will roll back the transaction 
 * by writing 'ABORT' in each participant node.Then each participant will be notified by the watch each one set on its own node;
 * 
 *      Sixthly,if all participants vote 'PREPARED', coordinator will commit the transaction by writing 'COMMIT' in each participant node;
 *      
 *      Seventhly, if a participant has successfully committed the local transaction, it writes 'COMMITTED' in its own node. If failed, 
 * commitment will be retried  a certain times until committed.  If the transaction remains uncommitted or a crash occurred before writing
 * 'COMMITTED', coordinator will alert the system to get human involved;
 * 
 *      Eighthly,if all participants are committed, the transaction node and its children will be removed.
 *      
 * 
 * 
 * @author zhangxu
 * @version $Id: DTCoordinator.java, v 0.1 2015年9月7日 下午9:00:29 zhangxu Exp $
 */
/**
 * 
 * @author zhangxu
 * @version $Id: DXCoordinator.java, v 0.1 2015年9月9日 下午4:31:57 zhangxu Exp $
 */
public class DXCoordinator implements Watcher {

    private static final Logger LOG             = LoggerFactory
                                                    .getLogger(DXCoordinator.class);

    private ZooKeeper session;
    private static final String DX_PREFIX       = "dx";
    private static final String ZK_SERVER_URL   = "localhost:1281";
    private static final int    SESSION_TIMEOUT = 9000;

    private String              currentXDir;

    /**
     * initialize the session to ZK server;
     */
    public void start() {
        try {
            session = new ZooKeeper(ZK_SERVER_URL, SESSION_TIMEOUT, this);

        } catch (IOException e) {

        }
    }

    /**
     * close the session to ZK server and release resources
     */
    public void destroy() {

        try {

            session.close();
            session = null;

        } catch (InterruptedException e) {

            LOG.error("Get interrupted while closing the session to ZK Server.", e);
        }
    }

    /** 
     * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
     * 
     * watch for session changes
     */
    @Override
    public void process(WatchedEvent event) {

        if (event.getType() != EventType.None)
            return;

        switch (event.getState()) {
            case SyncConnected:
                break;
            case Expired:
                break;
            case Disconnected:
                break;

            default:
                break;
        }
    }


    /**
     * Duty One:
     * 
     * start a distributed transaction by creating a unique transaction directory 
     */
    public void beginX() {

        session.create(DX_PREFIX, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
            createXDirCallback,  null);
    }

    /**
     * callback for ZK.create in method beignX.
     * 
     * guarantee that XDir is successfully created
     */
    StringCallback createXDirCallback = new StringCallback() {

                                          @Override
                                          public void processResult(int rc, String path,
                                                                    Object ctx, String name) {
                                              currentXDir = name;
                                          }
                                      };
                                      
    /**
     * Duty Two:
     *      
     *      Notify all participants to prepare a transaction by writing 'PREPARE' in each participant node.     
     */
    public void notifyToPrepareX() {

        notifyParticipants(CoordinatorOrder.PREPARE);
    }

    /**
     * notify all participants to execute a specific order.
     * @param order
     */
    private void notifyParticipants(CoordinatorOrder order) {

        session.getChildren(currentXDir, participantsWatcher, notifyParticipantsCallback, order);
    }

    /**
     * watcher for ZK.getChildren in method notifyToPrapareX.
     * 
     * watch the participants collection:
     *          if there is a node disappearance or vote 'ABORT', abort the whole transaction.
     *          if all participants vote 'PREPARE', commit the whole transaction.
     */
    Watcher participantsWatcher = new Watcher() {

                                                    @Override
                                                    public void process(WatchedEvent event) {

                                                        if (event.getType() == EventType.NodeChildrenChanged) {

                                                            abortX();
                                                        } else if (event.getType() == EventType.NodeCreated) {

                                                            //if one votes 'ABORT', abort the X
                                                            //if all vote 'PREPARE' commit the X
                                                            //else continue watching
                                                            commitX();
                                                        }
                                                    }
                                                };

    /** 
     * callback for ZK.getChildren in method notifyToPrepareX.
     *  
     *  write 'PREPARE' to each participant node to notify them to prepare for the current transaction.
     *  */
    ChildrenCallback notifyParticipantsCallback = new ChildrenCallback() {

                                                    @Override
                                                    public void processResult(int rc, String path,
                                                                              Object ctx,
                                                                              List<String> children) {

                                                        //notify each participant to prepare the X
                                                        writeMsgToAllParticipantNodes(children,
                                                            (CoordinatorOrder) ctx);
                                                    }
                                                };

    /**
     * write message to each child in children
     * 
     * @param children
     * @param order
     */
    private void writeMsgToAllParticipantNodes(List<String> children, CoordinatorOrder order) {
        for (String participantPath : children) {
            session.setData(participantPath, order.toString().getBytes(), -1,
                writeParticipantNodeCallback, null);
        }
    }

    /** 
     * callback for ZK.setData in method writeMsgToAllParticipantNodes
     * 
     * make sure that each participant get notified by the message sent by the coordinator
     *  */
    StatCallback writeParticipantNodeCallback = new StatCallback() {

                                                  @Override
                                                  public void processResult(int rc, String path,
                                                                            Object ctx, Stat stat) {
                                                  }
                                              };

    /**
     * Duty Three:
     * 
     *      watch all participants' votes and decide to abort or commit the transaction;
     */
    public void decideX() {

        //do nothing, because the participantWatcher set before will do this.
    }

    /**
     * notify all participants to abort the transaction
     */
    private void abortX() {
        notifyParticipants(CoordinatorOrder.ABORT);
    }

    /**
     * notify all participants to commit the transaction
     */
    private void commitX() {
        notifyParticipants(CoordinatorOrder.COMMIT);
    }

    /**
     * Duty four:
     * 
     *      clear and release relative resources if X succeeded finally;
     *      or ask for human intervention by alerting
     */
    public void closeX() {

        //do nothing, because the participantWatcher set before will do this.
    }
}
