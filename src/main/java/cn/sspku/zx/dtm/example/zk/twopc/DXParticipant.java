/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2015 All Rights Reserved.
 */
package cn.sspku.zx.dtm.example.zk.twopc;

import java.io.IOException;
import java.nio.file.Paths;

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

import cn.sspku.zx.dtm.example.zk.twopc.XMessage.ParticipantReport;

/**
 * DTParticipant acts as the  participant of distributed transaction
 * 
 *      Functionality:
 *          1>register itself to the coordinator by add a node in the certain transaction directory;
 *          2>prepare the transaction and vote;
 *          3>commit or abort the transaction.
 *  
 * @author zhangxu
 * @version $Id: DTParticipant.java, v 0.1 2015年9月7日 下午9:38:44 zhangxu Exp $
 */
public class DXParticipant implements Watcher {

    private static final Logger LOG             = LoggerFactory.getLogger(DXParticipant.class);

    private ZooKeeper           session;
    private static final String DX_PREFIX       = "dx";
    private static final String PARTICIPANT_PREFIX = "participant-";
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
     *     register itself to the coordinator by add a node in the certain transaction directory;
     */
    public void registerToX() {

        session.create(currentXDir, new byte[0], Ids.OPEN_ACL_UNSAFE,
            CreateMode.EPHEMERAL_SEQUENTIAL, registerToXCallback, null);
    }

    /**
     * callback for ZK.create in method registerToX.
     * 
     * guarantee that participant node is successfully created
     */
    StringCallback registerToXCallback = new StringCallback() {

                                           @Override
                                           public void processResult(int rc, String path,
                                                                     Object ctx, String name) {
                                               currentXDir = name;
                                           }
                                       };

    /**
     * Duty Two:
     *      
     *      prepare the transaction and vote;
     *      if exception occurred in business activity, vote 'ABORTED'
     *      else vote 'PREPARED'
     */
    public void prepareForX() {

        try {

            doBusinessPrepare();

        } catch (Exception e) {

            voteAborted();
        }

        votePrepared();
    }

    /**
     * vote 'PREPARED' in prepare phrase
     */
    private void votePrepared() {

        writeMsgToParticipantNode(ParticipantReport.PREPAERD);
    }

    /**
     * vote 'ABORTED' in prepare phrase
     */
    private void voteAborted() {

        writeMsgToParticipantNode(ParticipantReport.ABORTED);
    }

    /**
     * vote 'COMMITED' in prepare phrase
     */
    private void voteCommitted() {

        writeMsgToParticipantNode(ParticipantReport.COMMITED);
    }

    /**
     * write message to participant node
     * 
     * @param report
     */
    private void writeMsgToParticipantNode(ParticipantReport report) {
        
        session.setData(Paths.get(DX_PREFIX, PARTICIPANT_PREFIX).toString(), report.toString()
            .getBytes(), -1, writeParticipantNodeCallback, report);
    }

    /** 
     * callback for ZK.setData in method writeMsgToParticipantNodes]
     * 
     * make sure that the coordinator get notified by the message sent by the participant
     *  */
    StatCallback writeParticipantNodeCallback = new StatCallback() {

                                                  @Override
                                                  public void processResult(int rc, String path,
                                                                            Object ctx, Stat stat) {
                                                  }
                                              };

    /**
     * do business activity
     */
    protected void doBusinessPrepare() {
    }

    /**
     * commit the transaction
     */
    public void commitX() {
        try {
            doBusinessCommit();

        } catch (Exception e) {
            commitX();
        }

        voteCommitted();
    }

    /**
     * do the real business commitment job
     */
    protected void doBusinessCommit() {
    }
}
