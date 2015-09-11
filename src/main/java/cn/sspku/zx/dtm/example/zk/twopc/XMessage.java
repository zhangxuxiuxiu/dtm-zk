/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2015 All Rights Reserved.
 */
package cn.sspku.zx.dtm.example.zk.twopc;

/**
 * define message constants used in communication between coordinator and participant
 * 
 * @author zhangxu
 * @version $Id: XMessage.java, v 0.1 2015年9月9日 下午4:14:54 zhangxu Exp $
 */
public interface XMessage {

    enum CoordinatorOrder {
        PREPARE, COMMIT, ABORT
    }

    enum ParticipantReport {
        PREPAERD, COMMITED, ABORTED
    }
}
