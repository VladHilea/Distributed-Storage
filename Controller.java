import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


/*
TODO Trebuie indexat asa ( filename - {dstoreuri , ack , "store in progress"} )
TODO Trebuie probabil
 */

public class Controller {
    public void setCport(int cport) {
        this.cport = cport;
    }

    public void setR_factor(int r_factor) {
        R_factor = r_factor;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public void setRebalancePeriod(int rebalancePeriod) {
        this.rebalancePeriod = rebalancePeriod;
    }

    public void setAllDStoresConeccted(ArrayList allDStoresConeccted) {
        this.allDStoresConeccted = allDStoresConeccted;
    }

    public void setFilesInTheSystem(ArrayList<String> filesInTheSystem) {
        this.filesInTheSystem = filesInTheSystem;
    }

    public void setSsController(ServerSocket ssController) {
        this.ssController = ssController;
    }

    public int getCport() {
        return cport;
    }

    public int getR_factor() {
        return R_factor;
    }

    public int getTimeout() {
        return timeout;
    }

    public int getRebalancePeriod() {
        return rebalancePeriod;
    }

    public ArrayList<Integer> getAllDStoresConeccted() {
        return allDStoresConeccted;
    }

    public ArrayList<String> getFilesInTheSystem() {
        return filesInTheSystem;
    }

    public Socket getClientConnection() {
        return clientConnection;
    }

    public void setClientConnection(Socket clientConnection) {
        this.clientConnection = clientConnection;
    }

    public  <K, V> K getKey(Map<K, V> map, V value)
    {
        for (K key: map.keySet())
        {
            if (value.equals(map.get(key))) {
                return key;
            }
        }
        return null;
    }



    public String getFilesList(){
        String list="";
        for(String i : getFilesInTheSystem()){
            list= list + i +" ";
        }
        return list;
    }

    public ServerSocket getSsController() {
        return ssController;
    }

    Socket clientConnection = new Socket();
    boolean keepOpen = false;
    int loading_dstore=0;
    int cport;
    int R_factor;
    int timeout;
    int rebalancePeriod;
    int dstorecounter = 0;
    ArrayList<String> queueOfOperations = new ArrayList<String>();
    ArrayList<Integer> allDStoresConeccted = new ArrayList<Integer>();
    ArrayList<String> filesInTheSystem = new ArrayList<String>();
    ServerSocket ssController;
    String index= ""; // 0 is free;
    public AtomicInteger ackDsores = new AtomicInteger(20);
    int ackDStores;
    int ackDStoresRemove;
    HashMap<Integer,Socket> connections = new HashMap<>();


    HashMap<String,ArrayList<Integer>> fileDstores = new HashMap<>();
    HashMap<String,String> fileIndex = new HashMap<>();
     HashMap<String,Integer> fileack= new HashMap<>();



    HashMap<String,Integer> fileRemoveack= new HashMap<>();


    public String getIndex(String filename){
        return fileIndex.get(filename);
    }

    public synchronized int getACK(String filename){
        return fileack.get(filename);
    }
    public ArrayList<Integer> getDstores (String filename){
        return fileDstores.get(fileDstores);
    }

    public synchronized HashMap<String, ArrayList<Integer>> getFileDstores() {
        return fileDstores;
    }

    public synchronized HashMap<String, String> getFileIndex() {
        return fileIndex;
    }

    public synchronized HashMap<String, Integer> getFileack() {
        return fileack;
    }

    public synchronized HashMap<String, Integer> getFileRemoveack() {
        return fileRemoveack;
    }

    public Controller(int cport, int r_factor, int timeout, int rebalancePeriod) {

        try {
            ControllerLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.cport = cport;
        this.R_factor = r_factor;
        this.timeout = timeout;
        this.rebalancePeriod = rebalancePeriod;
        setReceiver(cport);
        listen();

    }

    public void setReceiver(int cport) {
        try {
            ssController = new ServerSocket(cport);
            //ssController.setSoTimeout(100);
            ssController.setSoTimeout(getTimeout());
        } catch (IOException e) {
            System.out.println("error " + e);
        }
    }




    public void listen() {

        for (;;){
            try {
                Socket client = ssController.accept();

                new Thread(new Runnable(){
                    public void run(){try{
                        BufferedReader in = new BufferedReader(
                                new InputStreamReader(client.getInputStream()));
                        String message;
                        while((message = in.readLine()) != null) {

                            System.out.println(message + "----------received");

                            String[] splittedMessage = message.split("\\s");
                            String infoMessage = splittedMessage[0];


                            if (infoMessage.equals("JOIN")) {
                                System.out.println(client);
                                for(int i : connections.keySet()){
                                    System.out.println(i);
                                    System.out.println(connections.get(i));
                                }
                                System.out.println(client.getPort());

                                System.out.println("DStore with port " + Integer.parseInt(splittedMessage[1]) + " joined.");
                                connections.put(Integer.parseInt(splittedMessage[1]), new Socket("localhost",Integer.parseInt(splittedMessage[1])));
                                send(connections.get(Integer.parseInt(splittedMessage[1])),"ACK");
                                getAllDStoresConeccted().add(Integer.parseInt(splittedMessage[1]));
                                dstorecounter++;
                            }


                            if (infoMessage.equals("STORE")) {

                                if (getClientConnection() == null) {
                                    setClientConnection(client);
                                }

                                if (getAllDStoresConeccted().size() < getR_factor()) {
                                    System.out.println("Not enough Dstores");
                                    send(client, "ERROR_NOT_ENOUGH_DSTORES");
                                } else {

                                    String filename;
                                    filename = splittedMessage[1];


                                    if (!fileIndex.containsKey(filename)) {


                                            fileIndex.put(filename, "storing in progress"); //se pune in file index care va tine minte toate fileurile

                                            System.out.println(client.toString());


                                            fileack.put(filename,0);

                                        System.out.println(fileack.get(filename));


                                                //System.out.println(ackDStores);
                                            String dstoresChoosen = "";
                                            ArrayList<Integer> chosenDstorelist = getRDstores(getR_factor());
                                            for (int i : chosenDstorelist) {
                                                    dstoresChoosen = dstoresChoosen + i + " ";
                                            }
                                            fileDstores.put(filename, chosenDstorelist);//fiecare file in ce dstore e pus

                                            send(client, "STORE_TO " + dstoresChoosen);
                                            System.out.println(fileack.get(filename));



                                            //Thread.sleep(200);


                                            long time = System.currentTimeMillis();
                                            while (fileack.get(filename) < getR_factor() && System.currentTimeMillis() < time + getTimeout()) {
                                                //System.out.println(fileack.get(filename));
                                                //Thread.sleep(100);
                                            }


                                            //System.out.println(ackDStores);
                                            System.out.println(fileack.get(filename));
                                            if (fileack.get(filename) < getR_factor()) {

                                                //logger eroare
                                                //setIndex("");
                                                System.out.println("Timeout Error, Didn t receive all ack for " + filename);
                                                fileIndex.remove(filename);
                                                fileDstores.remove(filename);
                                            } else {
                                                fileIndex.put(filename,"store complete");
                                                send(client, "STORE_COMPLETE");
                                            }




                                    } else if(getIndex(filename).equals("storing in progress") || getIndex(filename).equals("store complete") || getIndex(filename).equals("removing in progress") ){
                                        send(client,"ERROR_FILE_ALREADY_EXISTS");
                                    } else if(getIndex(filename).equals("remove complete") ) {
                                        fileIndex.put(filename, "storing in progress"); //se pune in file index care va tine minte toate fileurile

                                        System.out.println(client.toString());


                                        fileack.put(filename,0);


                                        //System.out.println(ackDStores);
                                        String dstoresChoosen = "";
                                        ArrayList<Integer> chosenDstorelist = getRDstores(getR_factor());
                                        for (int i : chosenDstorelist) {
                                            dstoresChoosen = dstoresChoosen + i + " ";
                                        }
                                        send(client, "STORE_TO " + dstoresChoosen);

                                        fileDstores.put(filename, chosenDstorelist);//fiecare file in ce dstore e pus


                                        //Thread.sleep(200);
                                        long time = System.currentTimeMillis();
                                        while (fileack.get(filename) < getR_factor() && System.currentTimeMillis() < time + getTimeout()) {
                                            //System.out.println(ackDsores.get());
                                            //Thread.sleep(100);
                                        }


                                        //System.out.println(ackDStores);
                                        if (fileack.get(filename) < getR_factor()) {

                                            //logger eroare
                                            System.out.println("Timeout Error, Didn't Receive all Store ACK. Removing file....");
                                            fileIndex.remove(filename);
                                            fileDstores.remove(filename);
                                        } else {
                                            fileIndex.put(filename,"store complete");
                                            send(client, "STORE_COMPLETE");
                                        }
                                    }


                                }
                            }

                            if (infoMessage.equals("STORE_ACK")) {
                                String filename;
                                filename = splittedMessage[1];
                                fileack.put(filename,fileack.get(filename)+1);
                                //Thread.sleep(100);
                            }

                            if (infoMessage.equals("LIST")) {

                                if (getClientConnection() == null) {
                                    setClientConnection(client);
                                }

                                if (getAllDStoresConeccted().size() < getR_factor()) {
                                    System.out.println("Not enough Dstores");
                                    send(client, "ERROR_NOT_ENOUGH_DSTORES");
                                } else {
                                    String list= "";

                                    for(String s : fileIndex.keySet()){
                                        if(!(getIndex(s).equals("storing in progress") || getIndex(s).equals("remove complete") || getIndex(s).equals("removing in progress"))){ //nush dc nu merge fara asta bine
                                            list = list + s + " ";
                                            System.out.println(s + " - " +fileDstores.get(s));
                                        }


                                    }

                                    send(client, "LIST " + list); //lasa asta aici

                                    System.out.println("Index is : " + fileIndex + "\n\r");
                                    System.out.println("File distribution is: " + fileDstores);
                                }
                            }





                            if (infoMessage.equals("LOAD")){
                                if (getClientConnection() == null) {
                                    setClientConnection(client);
                                }

                                loading_dstore=0;
                                String filename = splittedMessage[1];
                                if (getAllDStoresConeccted().size() < getR_factor()) {
                                    System.out.println("Not enough Dstores");
                                    send(client, "ERROR_NOT_ENOUGH_DSTORES");
                                } else if(fileIndex.containsKey(filename)){
                                    if (getIndex(filename).equals("storing in progress") || getIndex(filename).equals("remove complete") || getIndex(filename).equals("removing in progress")){
                                        System.out.println("File doesn't exist, because the index is updating");
                                        send(client, "ERROR_FILE_DOES_NOT_EXIST");
                                    } else {
                                        ArrayList<Integer> dstoresOfTheFile = fileDstores.get(filename);
                                        long size=0;
                                        File file = new File(filename);
                                        if(file.exists() || file.isFile()){
                                            size = file.length();
                                        }
                                        if(dstoresOfTheFile.size() > 0)
                                        send(client,"LOAD_FROM "+ dstoresOfTheFile.get(loading_dstore) + " " + size);
                                    }
                                } else {
                                    System.out.println("File doesn't exist in the index");
                                    send(client, "ERROR_FILE_DOES_NOT_EXIST");
                                }



                            }

                            if (infoMessage.equals("RELOAD")){

                                if (getClientConnection() == null) {
                                    setClientConnection(client);
                                }

                                loading_dstore++;
                                String filename = splittedMessage[1];

                                    ArrayList<Integer> dstoresOfTheFile = fileDstores.get(filename);
                                    long size=0;
                                    File file = new File(filename);
                                    if(file.exists() || file.isFile()){
                                        size = file.length();
                                    }
                                    if(loading_dstore > dstoresOfTheFile.size()-1) {
                                        send(client, "LOAD_FROM " + dstoresOfTheFile.get(loading_dstore) + " " + size);
                                    } else {
                                        send(client,"ERROR_LOAD");
                                    }

                            }


                            if (infoMessage.equals("REMOVE")){



                                if (getAllDStoresConeccted().size() < getR_factor()) {
                                    System.out.println("Not enough Dstores");
                                    send(client, "ERROR_NOT_ENOUGH_DSTORES");
                                } else {

                                    String filename = splittedMessage[1];

                                    if (fileIndex.containsKey(filename)) {
                                        if(getIndex(filename).equals("store complete")){

                                            fileIndex.put(filename,"removing in progress");


                                           fileRemoveack.put(filename,0);



                                            //System.out.println(ackDStoresRemove);

                                            String dstoresChoosen = "";
                                            ArrayList<Integer> chosenDstorelist = fileDstores.get(filename);
                                            for (int i : chosenDstorelist) {
                                                //sendPort("REMOVE " + filename , i);
                                                send(connections.get(i), "REMOVE " + filename);
                                                System.out.println(connections.get(i));
                                            }

                                            //System.out.println(getFilesInTheSystem());
                                            //System.out.println(fileDstores);
                                            //send(client, "STORE_TO " + dstoresChoosen);

                                            //fileDstores.put(filename,chosenDstorelist);


                                            //fileDstores.remove(filename);
                                            //getFilesInTheSystem().remove(filename);

                                            //Thread.sleep(200);

                                            long time = System.currentTimeMillis();
                                            while (fileRemoveack.get(filename) < getR_factor() && System.currentTimeMillis() < time + getTimeout()) {
                                                //System.out.println(ackDsores.get());
                                                //Thread.sleep(100);
                                            }


                                            //System.out.println(ackDStoresRemove);



                                            if (fileRemoveack.get(filename) < getR_factor()) {

                                                //logger eroare
                                                System.out.println("Timeout Error, Didn't Receive all Remove ACK");
                                                //fileIndex.remove(filename);

                                            } else {

                                                fileDstores.remove(filename);
                                                fileIndex.put(filename,"remove complete");
                                                System.out.println(getFilesInTheSystem());
                                                System.out.println(fileDstores);

                                                send(client, "REMOVE_COMPLETE");
                                            }

                                        } else {
                                            send(client, "ERROR_FILE_DOES_NOT_EXIST");
                                        }

                                    } else {
                                        System.out.println("Trying to remove a file that i snot curently stored" + " " + filename);
                                        //does not exist error
                                        send(client, "ERROR_FILE_DOES_NOT_EXIST");
                                    }
                                }
                            }

                            if(infoMessage.equals("REMOVE_ACK")){

                                String filename;
                                filename = splittedMessage[1];
                                fileRemoveack.put(filename,fileRemoveack.get(filename)+1);
                            }







                        } client.close();


                    }catch(Exception e){}
                    }
                }).start();
            } catch (Exception e) {

            }
        }
    }



    public static HashMap<Integer, Integer> sortByValue(HashMap<Integer, Integer> hm)
    {
        // Create a list from elements of HashMap
        List<Map.Entry<Integer, Integer> > list =
                new LinkedList<Map.Entry<Integer, Integer> >(hm.entrySet());

        // Sort the list
        Collections.sort(list, new Comparator<Map.Entry<Integer, Integer> >() {
            public int compare(Map.Entry<Integer, Integer> o1,
                               Map.Entry<Integer, Integer> o2)
            {
                return (o1.getValue()).compareTo(o2.getValue());
            }
        });

        // put data from sorted list to hashmap
        HashMap<Integer, Integer> temp = new LinkedHashMap<Integer, Integer>();
        for (Map.Entry<Integer, Integer> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }
        return temp;
    }

    public ArrayList<Integer> getRDstores(int R){
        HashMap<Integer,Integer> DstoreOccurence= new HashMap<>();

        for(Integer i : connections.keySet()){
            DstoreOccurence.put(i,0);
            for(ArrayList<Integer> list: fileDstores.values()){
                if(list.contains(i)){
                    DstoreOccurence.put(i,DstoreOccurence.get(i)+1);
                }
            }
        }

        Map<Integer,Integer> sortedDstores = sortByValue(DstoreOccurence);

        ArrayList<Integer> rDstores = new ArrayList<Integer>();

        int n=getR_factor();
        for(int i : sortedDstores.keySet()){
            rDstores.add(i);
            n--;
            if(n==0){
                break;
            }
        }



        return rDstores;
    }

    public void setIndex(String index){
        this.index = index;
    }

    public String getIndex(){
        return this.index;
    }

   /* public void send(String message, int port){
        try{Socket socket = new Socket("localhost",port);
            PrintWriter out = new PrintWriter(socket.getOutputStream());

            out.println(message); out.flush();
            System.out.println("TCP message: "+message+"  -------sent");


        }catch(Exception e){System.out.println("error"+e);}
    }

    */

    public void send(Socket socket, String message){

                  try {
                      PrintWriter out = new PrintWriter(socket.getOutputStream());

                      out.println(message);
                      out.flush();
                      System.out.println("TCP message: " + message + "  -------sent thourgh socket" + socket.toString());

                      //Thread.sleep(200);


                  } catch (Exception e) {
                      System.out.println("error" + e);
                  }
    }

/*
    public static void sendPort(String message, int port){
        try{Socket socket = new Socket("localhost", port);
            PrintWriter out = new PrintWriter(socket.getOutputStream());

            out.println(message); out.flush();
            System.out.println("TCP message: "+message+"  -------sent");

            //Thread.sleep(100);


        }catch(Exception e){System.out.println("error"+e);}
    }



 */



    public static void main(String[] args) {
        int cportArg = Integer.parseInt(args[0]);
        int rFactorArg = Integer.parseInt(args[1]);
        int timeoutArg = Integer.parseInt(args[2]);
        int rebalanceArg = Integer.parseInt(args[3]);

        new Controller(cportArg,rFactorArg,timeoutArg,rebalanceArg);

    }


}


