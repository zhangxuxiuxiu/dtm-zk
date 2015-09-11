package cn.sspku.zx.dtm.example.zk;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * 
 * @author zhangxu
 * @version $Id: SyncPrimitive.java, v 0.1 2015年9月7日 下午5:29:27 zhangxu Exp $
 */
/**
 * 
 * @author zhangxu
 * @version $Id: SyncPrimitive.java, v 0.1 2015年9月7日 下午5:30:37 zhangxu Exp $
 */
public class SyncPrimitive implements Watcher {

    static ZooKeeper zk = null;
    static Integer mutex;

    String parent_node_name;

    
    /**
     * construct a zookeeper client with the parameter address if the client does not exist
     * @param address
     */
    SyncPrimitive(String address) {
        if(zk == null){
            try {
                System.out.println("Starting ZK:");
                zk = new ZooKeeper(address, 3000, this);
                mutex = new Integer(-1);
                System.out.println("Finished starting ZK: " + zk);
            } catch (IOException e) {
                System.out.println(e.toString());
                zk = null;
                System.out.println("Failed in  starting  ZK!!!");
            }
        }       
    }

    /** 
     * when a watch event triggers on the observed node on the ZK server,
     * this callback will be invoked
     * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
     */
    synchronized public void process(WatchedEvent event) {
        synchronized (mutex) {
            System.out.println("Watch Event:" + event.getType()+" is being processed!!!");
            mutex.notify();
        }
    }

    /**
     * Barrier
     */
    static public class Barrier extends SyncPrimitive {
                
        int max_capacity;
        String barrier_child_node_path;
        
        /**
         * Barrier constructor
         *
         * @param zk_server_addr
         * @param barrier_parent_node_name
         * @param max_capacity
         */
        Barrier(String zk_server_addr, String barrier_parent_node_name, int max_capacity) {
            super(zk_server_addr);
            this.parent_node_name = barrier_parent_node_name;
            this.max_capacity = max_capacity;

            // Create barrier parent node if it does not exist.
            if (zk != null) {
                try {
                    Stat s = zk.exists(barrier_parent_node_name, false);
                    if (s == null) {
                        zk.create(barrier_parent_node_name, new byte[0], Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT);
                    }
                } catch (KeeperException e) {
                    System.out
                            .println("Keeper exception when instantiating queue: "
                                    + e.toString());
                } catch (InterruptedException e) {
                    System.out.println("Interrupted exception");
                }
            }

            // obtain local barrier child node name
            try {
                String barrier_child_node_name = new String(InetAddress.getLocalHost().getCanonicalHostName().toString());
                barrier_child_node_path=Paths.get(barrier_parent_node_name, barrier_child_node_name).toString();
                
            } catch (UnknownHostException e) {
                System.out.println(e.toString());
            }

        }

        /**
         * Join barrier
         *
         * @return
         * @throws KeeperException
         * @throws InterruptedException
         */

        boolean enter() throws KeeperException, InterruptedException{
            zk.create(barrier_child_node_path, new byte[0], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL);
            while (true) {
                synchronized (mutex) {
                    List<String> list = zk.getChildren(parent_node_name, true);

                    if (list.size() < max_capacity) {
                        mutex.wait();
                    } else {
                        return true;
                    }
                }
            }
        }
        
        /**
         * Wait until all reach barrier
         *
         * @return
         * @throws KeeperException
         * @throws InterruptedException
         */

        boolean leave() throws KeeperException, InterruptedException{
            
            zk.delete(barrier_child_node_path, 0);
            while (true) {
                synchronized (mutex) {
                    List<String> list = zk.getChildren(parent_node_name, true);
                        if (list.size() > 0) {
                            mutex.wait();
                        } else {
                            return true;
                        }
                    }
                }
        }
    }

    /**
     * Producer-Consumer queue
     */
    static public class Queue extends SyncPrimitive {

        private static final String ELE_PREFIX="element";
        
        /**
         * Constructor of producer-consumer queue
         *
         * @param zk_server_addr
         * @param queue_parent_node_name
         */
        Queue(String zk_server_addr, String queue_parent_node_name) {
            super(zk_server_addr);
            this.parent_node_name = queue_parent_node_name;
            
            // Create queue parent node if it does not exist.
            if (zk != null) {
                try {
                    Stat s = zk.exists(parent_node_name, false);
                    if (s == null) {
                        zk.create(parent_node_name, new byte[0], Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT);
                    }
                } catch (KeeperException e) {
                    System.out
                            .println("Keeper exception when instantiating queue: "
                                    + e.toString());
                } catch (InterruptedException e) {
                    System.out.println("Interrupted exception");
                }
            }
        }
        
        /**
         * Add element to the queue.
         *
         * @param i
         * @return
         */

        boolean produce(int i) throws KeeperException, InterruptedException{
            
            ByteBuffer b = ByteBuffer.allocate(4);
            byte[] value;           
            b.putInt(i);
            value = b.array();
            
            // Add child with value i to the queue
            zk.create(Paths.get(parent_node_name,ELE_PREFIX).toString(), value, Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT_SEQUENTIAL);

            return true;
        }


        /**
         * Remove first element from the queue.
         *
         * @return
         * @throws KeeperException
         * @throws InterruptedException
         */
        int consume() throws KeeperException, InterruptedException{
            int retvalue = -1;
            Stat stat = null;

            // Get the first element available and remove it 
            while (true) {
                
                synchronized (mutex) {
            
                    List<String> list = zk.getChildren(parent_node_name, true);
                    if (list.size() == 0) {
                        System.out.println("Going to wait");
                        mutex.wait();
                        
                    } else {
                        
                        // search the node with the minimum No 
                        Integer min_Num = new Integer(list.get(0).substring(ELE_PREFIX.length()));
                        for(String s : list){
                            Integer int_num = new Integer(s.substring(ELE_PREFIX.length()));
                            
                            if(int_num < min_Num) min_Num = int_num;
                        }
                        
                        //construct the child node name with minimum number 
                        String min_node_path= Paths.get(parent_node_name, ELE_PREFIX + min_Num).toString();                       
                        System.out.println("Minimum node: " +min_node_path);
                        //obtain the data in this child node and remove it 
                        byte[] b = zk.getData(min_node_path, false, stat);
                        zk.delete(min_node_path, 0);
                        ByteBuffer buffer = ByteBuffer.wrap(b);
                        retvalue = buffer.getInt();

                        return retvalue;
                    }
                }
            }
        }
    }
    
    public static void main(String args[]) {
        if (args[0].equals("qTest"))
            queueTest(args);
        else
            barrierTest(args);

    }

    public static void queueTest(String args[]) {
        Queue q = new Queue(args[1], "/app1");

        System.out.println("Input: " + args[1]);
        int i;
        Integer max = new Integer(args[2]);

        if (args[3].equals("p")) {
            System.out.println("Producer");
            for (i = 0; i < max; i++)
                try{
                    q.produce(10 + i);
                } catch (KeeperException e){

                } catch (InterruptedException e){

                }
        } else {
            System.out.println("Consumer");

            for (i = 0; i < max; i++) {
                try{
                    int r = q.consume();
                    System.out.println("Item: " + r);
                } catch (KeeperException e){
                    i--;
                } catch (InterruptedException e){

                }
            }
        }
    }

    public static void barrierTest(String args[]) {
        Barrier b = new Barrier(args[1], "/b1", new Integer(args[2]));
        try{
            boolean flag = b.enter();
            System.out.println("Entered barrier: " + args[2]);
            if(!flag) System.out.println("Error when entering the barrier");
        } catch (KeeperException e){

        } catch (InterruptedException e){

        }

        // Generate random integer
        Random rand = new Random();
        int r = rand.nextInt(100);
        // Loop for rand iterations
        for (int i = 0; i < r; i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {

            }
        }
        try{
            b.leave();
        } catch (KeeperException e){

        } catch (InterruptedException e){

        }
        System.out.println("Left barrier");
    }
}