import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

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
    HashMap<Integer,Socket> connections = new HashMap<>();

    HashMap<String,ArrayList<Integer>> fileDstores = new HashMap<>();




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
            ssController.setSoTimeout(5000);
            //ssController.setSoTimeout(getTimeout());
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
                                connections.put(client.getPort(), client);
                                System.out.println(client.getPort());
                                System.out.println("DStore with port " + Integer.parseInt(splittedMessage[1]) + " joined.");
                                getAllDStoresConeccted().add(Integer.parseInt(splittedMessage[1]));
                                dstorecounter++;
                            }


                            if (infoMessage.equals("STORE")) {
                                setClientConnection(client);
                                String indexString[];
                                indexString = getIndex().split("\\s");
                                if (getIndex().equals("")) {



                                    System.out.println(client.toString());
                                    String filename = splittedMessage[1];
                                    setIndex("storing_in_progress " + filename);
                                    if (getAllDStoresConeccted().size() < getR_factor()) {
                                        System.out.println("Not enough Dstores");
                                        send(client, "ERROR_NOT_ENOUGH_DSTORES");
                                        setIndex("");
                                    } else if (getFilesInTheSystem().contains(filename)) {

                                        System.out.println("file exists");
                                        send(client, "ERROR_FILE_ALREADY_EXISTS");
                                        setIndex("");
                                    } else {

                                        getFilesInTheSystem().add(filename);
                                        ackDStores = 0;
                                        System.out.println(ackDStores);
                                        String dstoresChoosen = "";
                                        ArrayList<Integer> chosenDstorelist = getRDstores(getR_factor());
                                        for (int i : chosenDstorelist) {
                                            dstoresChoosen = dstoresChoosen + i + " ";
                                        }
                                        send(client, "STORE_TO " + dstoresChoosen);

                                        fileDstores.put(filename,chosenDstorelist);


                                        Thread.sleep(100);
                                            long time = System.currentTimeMillis();
                                            while(ackDStores<getR_factor() && System.currentTimeMillis()<time+getTimeout()){
                                                //System.out.println(ackDsores.get());
                                                //Thread.sleep(100);
                                            }
                                        System.out.println(ackDStores);
                                            if(ackDStores < getR_factor()){

                                                //logger eroare
                                                setIndex("");
                                                getFilesInTheSystem().remove(filename);
                                            } else {
                                                setIndex("");
                                                send(client, "STORE_COMPLETE");
                                            }

                                    }

                                } else if( indexString[0].equals("storing_in_progress")){
                                    if(splittedMessage[1].equals(indexString[1])){
                                        send(client, "ERROR_FILE_ALREADY_EXISTS");
                                    } else {
                                        queueOfOperations.add(message);
                                        System.out.println(queueOfOperations.get(0));
                                    }

                                }
                            }

                            if (infoMessage.equals("STORE_ACK")) {
                                ackDStores++;
                                Thread.sleep(100);
                            }

                            if (infoMessage.equals("LIST")) {
                                if (getAllDStoresConeccted().size() < getR_factor()) {
                                    System.out.println("Not enough Dstores");
                                    send(client, "ERROR_NOT_ENOUGH_DSTORES");
                                } else {


                                    for(String i : getFilesInTheSystem()){
                                        System.out.println(i + " - " +fileDstores.get(i));
                                    }
                                    send(client, "LIST " + getFilesList());
                                }
                            }


                        }


                    }catch(Exception e){}
                    }
                }).start();
            } catch (Exception e) {

            }
        }
    }



    public ArrayList<Integer> getRDstores(int R){
        ArrayList<Integer> rDstores = new ArrayList<Integer>();
        for(int i = 0 ; i<R; i++){
            rDstores.add(getAllDStoresConeccted().get(i));
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


                  } catch (Exception e) {
                      System.out.println("error" + e);
                  }
    }





    public static void main(String[] args) {
        int cportArg = Integer.parseInt(args[0]);
        int rFactorArg = Integer.parseInt(args[1]);
        int timeoutArg = Integer.parseInt(args[2]);
        int rebalanceArg = Integer.parseInt(args[3]);

        new Controller(cportArg,rFactorArg,timeoutArg,rebalanceArg);

    }


}


