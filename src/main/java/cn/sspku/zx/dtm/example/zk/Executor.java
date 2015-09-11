package cn.sspku.zx.dtm.example.zk;

/**
 * A simple example program to use DataMonitor to start and
 * stop executables based on a znode. The program watches the
 * specified znode and saves the data that corresponds to the
 * znode in the filesystem. It also starts the specified program
 * with the specified arguments when the znode exists and kills
 * the program if the znode goes away.
 */
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class Executor implements Watcher, Runnable, DataMonitor.DataMonitorListener {

    String znode;

    DataMonitor dm;

    ZooKeeper zk;

    String filename;

    String exec[];

    Process     child;   //the executable task  created by executing the command 'exec[]'.

    public Executor(String hostPort, String znode, String filename,
            String exec[]) throws KeeperException, IOException {

        this.filename = filename;
        this.exec = exec;
        zk = new ZooKeeper(hostPort, 3000, this); //watch by itself
        dm = new DataMonitor(zk, znode, null, this);
    }
    


    /***************************************************************************
     * We do process any events ourselves, we just need to forward them on.
     *
     * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.proto.WatcherEvent)
     */
    public void process(WatchedEvent event) {

        //delegate the process logic to DataMonitor
        dm.process(event);
    }

    /** 
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        try {
            synchronized (this) {
                //while DataMonitor is not dead, Executor continues
                while (!dm.dead) {
                    wait();
                }
            }
        } catch (InterruptedException e) {
        }
    }

    /** 
     * @see cn.sspku.zx.dtm.example.zk.DataMonitor.DataMonitorListener#closing(int)
     * 
     * when Session expired or authorization failed, DataMonitor will trigger this method
     */
    public void closing(int rc) {
        synchronized (this) {
            //drive Executor to check if DataMonitor is dead, and this is done in the run method
            notifyAll();
        }
    }

    /** 
     * @see cn.sspku.zx.dtm.example.zk.DataMonitor.DataMonitorListener#exists(byte[])
     */
    public void exists(byte[] data) {

        //if  data is null, the executable will be killed
        if (data == null) {
            if (child != null) {
                System.out.println("Killing process");
                child.destroy();
                try {
                    child.waitFor();
                } catch (InterruptedException e) {
                }
            }
            child = null;
        }
        //else restart the executable
        else {
            //if the child process already exists, it will be stopped first
            if (child != null) {
                System.out.println("Stopping child");
                child.destroy();
                try {
                    child.waitFor();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            //record the data into the file 
            try {
                FileOutputStream fos = new FileOutputStream(filename);
                fos.write(data);
                fos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            //create or restart the child process to execute the 'exec' command
            try {
                System.out.println("Starting child");
                child = Runtime.getRuntime().exec(exec);
                new StreamWriter(child.getInputStream(), System.out);
                new StreamWriter(child.getErrorStream(), System.err);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * read data from first stream and write it into second stream
     * @author zhangxu
     * @version $Id: Executor.java, v 0.1 2015年9月7日 下午6:21:17 zhangxu Exp $
     */
    static class StreamWriter extends Thread {
        OutputStream os;

        InputStream  is;

        StreamWriter(InputStream is, OutputStream os) {
            this.is = is;
            this.os = os;
            start();
        }

        /** 
         * @see java.lang.Thread#run()
         */
        @Override
        public void run() {
            byte b[] = new byte[80];
            int rc;
            try {
                while ((rc = is.read(b)) > 0) {
                    os.write(b, 0, rc);
                }
            } catch (IOException e) {
            }

        }
    }


    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("USAGE: Executor hostPort znode filename program [args ...]");
            System.exit(2);
        }
        String hostPort = args[0];
        String znode = args[1];
        String filename = args[2];
        String exec[] = new String[args.length - 3];
        System.arraycopy(args, 3, exec, 0, exec.length);
        try {
            new Executor(hostPort, znode, filename, exec).run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}