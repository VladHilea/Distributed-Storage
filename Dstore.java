import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

public class Dstore {

    static int port;
    static int cport;
    static int timeout;
    static String filePath;
    static Controller controller;
    static ServerSocket ssDstore;
    static ArrayList<String> filesInDstore = new ArrayList<String>();


    public String  getListofFiles (ArrayList<File> files){
        String list=" ";
        for(File i : files){
            list= list + i.getName() + " ";
        }
        return list;
    }

    public File[]  getFilesReal(){
        File folder = new File(getFilePath());
        return folder.listFiles();
    }


    public static int getPort() {
        return port;
    }

    public static void setPort(int portA) {
        Dstore.port = portA;
    }

    public static int getCport() {
        return cport;
    }

    public static void setCport(int cport) {
        Dstore.cport = cport;
    }

    public static int getTimeout() {
        return timeout;
    }

    public static void setTimeout(int timeout) {
        Dstore.timeout = timeout;
    }

    public static String getFilePath() {
        return filePath;
    }

    public static void setFilePath(String filePath) {
        Dstore.filePath = filePath;
    }

    public static Controller getController() {
        return controller;
    }

    public void setController(Controller controller) {
        this.controller = controller;
    }

    public static ServerSocket getssDstore() {
        return ssDstore;
    }

    public void setssDstore(ServerSocket ssDstore) {
        this.ssDstore = ssDstore;
    }

    public static ServerSocket getSsDstore() {
        return ssDstore;
    }

    public static void setSsDstore(ServerSocket ssDstore) {
        Dstore.ssDstore = ssDstore;
    }

    public static  void setReceiver() {
        try {
            Dstore.ssDstore = new ServerSocket(getPort());
            //ssDstore.setSoTimeout(getTimeout());
        } catch (IOException e) {
            System.out.println("error " + e);
        }
    }

    public static  void connectController() {
        send("JOIN" + " " + getPort(), getCport());
    }

    public static ArrayList<String> getFilesInDstore() {
        return filesInDstore;
    }

    public static void listen() {
        for(;;) {
            try {

                Socket client = ssDstore.accept();
                new Thread(new Runnable(){
                    public void run(){try{
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(client.getInputStream()));
                String message;
                while ((message = in.readLine()) != null) {
                    System.out.println(message);
                    String[] splittedMessage = message.split("\\s");
                    String infoMessage = splittedMessage[0];

                    if (infoMessage.equals("STORE")) {
                        System.out.println(splittedMessage[1]);
                        getFilesInDstore().add(splittedMessage[1]);
                        send(client, "ACK");
                        listenFile(client,splittedMessage[1]);
                        send("STORE_ACK " + splittedMessage[1], getCport());
                    }
                }


                client.close();}catch(Exception e){}
                    }
                }).start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void listenFile(Socket socket,String filename){
        try {

            byte []b= new byte[200000];
            InputStream in = socket.getInputStream();
            FileOutputStream fr= new FileOutputStream(new File(getFilePath() + "/" + filename));
            in.readNBytes(b, 0, b.length);
            fr.write(b,0,b.length);




        } catch (Exception e) {
            System.out.println("error " + e);
        }
    }

    public static void sendFile(Socket client){

    }



    public static void send(String message, int port){
        try{Socket socket = new Socket("localhost", port);
            PrintWriter out = new PrintWriter(socket.getOutputStream());

            out.println(message); out.flush();
            System.out.println("TCP message: "+message+"  -------sent");


        }catch(Exception e){System.out.println("error"+e);}
    }

    public static void send(Socket socket, String message){
        try{
            PrintWriter out = new PrintWriter(socket.getOutputStream());

            out.println(message); out.flush();
            System.out.println("TCP message: "+message+"  -------sent thourgh socket" + socket.getInetAddress().getAddress().toString());
            Thread.sleep(1000);

        }catch(Exception e){System.out.println("error"+e);}
    }

    public static void main(String[] args){
        int portArgs = Integer.parseInt(args[0]);
        int cportRgs = Integer.parseInt(args[1]);
        int timeoutArg = Integer.parseInt(args[2]);

        String filePath = args[3];
        setPort(portArgs);
        setCport(cportRgs);
        setTimeout(timeoutArg);
        try {
            DstoreLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL, Dstore.port);
        } catch (IOException e) {
            e.printStackTrace();
        }

        setReceiver();


        setFilePath(filePath);
        String fileName = filePath;

        Path path = Paths.get(fileName);

        if (!Files.exists(path)) {
            try {
                Files.createDirectory(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("Directory created");
        } else {

            System.out.println("Directory already exists");
        }
        System.out.println(String.valueOf(getPort()));
        connectController();

        listen();




    }


}


