import java.io.*;
import java.net.*;
class TCPSender1{
    public static void main(String [] args){
        try{Socket socket = new Socket("localhost",4322);
            PrintWriter out = new PrintWriter(socket.getOutputStream());
            for(int i=0;i<10;i++){
                out.println("TCP message "+i); out.flush();
                System.out.println("TCP message "+i+" sent");
                Thread.sleep(1000);
            }
        }catch(Exception e){System.out.println("error"+e);}
    }
}
