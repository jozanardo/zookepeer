import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class Servidor {

    public static final String ipLocal = "127.0.0.1";
    private String ip;
    private int port;
    private String ipLeader;
    private int portLeader;
    private Boolean isLeader;
    private List<Integer> serverPorts;
    private Map<String, Mensagem> keyValueStorageTable = new HashMap<>();
    private static Scanner scanner = new Scanner(System.in);

    // Construtor da classe Servidor, recebe informa��es sobre o servidor e o l�der
    public Servidor(String ip, int port, String ipLeader, int portLeader, List<Integer> serverPorts) {
        this.ip = ip;
        this.port = port;
        this.ipLeader = ipLeader;
        this.portLeader = portLeader;
        this.isLeader = false;
        this.serverPorts = serverPorts;
    }

    // M�todos get e set para verificar e definir o status do servidor como l�der
    public Boolean getisLeader() {
        return isLeader;
    }

    public void setIsLeader(Boolean isLeader) {
        this.isLeader = isLeader;
    }

    // Fun��o para inicializar o servidor e aguardar requisi��es dos clientes
    public void initialize() {
        try (ServerSocket servidorSocket = new ServerSocket(port)) {
            while (true) {
                // Aceita conex�es dos clientes e cria uma nova thread para atender a cada requisi��o
                Socket clientSocket = servidorSocket.accept();
                ThreadAtendimento thread = new ThreadAtendimento(this, clientSocket);
                thread.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Fun��o para receber a porta do servidor e garantir que n�o esteja em uso
    private static int receiveServerPort() {
        int port;
        while (true) {
            System.out.print("Porta do servidor atual: ");
            port = Integer.parseInt(scanner.nextLine());
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                serverSocket.close();
                break;
            } catch (IOException e) {
                System.out.println("Porta " + port + " em uso. Por favor tente outra.");
            }
        }
        return port;
    }

    // Fun��o para obter as portas dos outros servidores com base na porta do l�der
    private static List<Integer> getPortValues(Integer leader) {
        List<Integer> portValues = new ArrayList<>();
        switch (leader) {
            case 10097:
                portValues = Arrays.asList(10098, 10099);
                break;
            case 10098:
                portValues = Arrays.asList(10097, 10099);
                break;
            case 10099:
                portValues = Arrays.asList(10097, 10098);
                break;
            default:
                break;
        }

        return portValues;
    }

    // Classe interna ThreadAtendimento que representa uma thread para atender a uma requisi��o do cliente
    public class ThreadAtendimento extends Thread {
        private Servidor server;
        private Socket clientSocket;

        public ThreadAtendimento(Servidor server, Socket clientSocket) {
            this.clientSocket = clientSocket;
            this.server = server;
        }

        public void run() {
            try {
                // Cria��o dos fluxos de entrada e sa�da de objetos para comunica��o com o cliente
                ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());

                // Leitura da mensagem enviada pelo cliente
                Mensagem message = (Mensagem) in.readObject();

                // Extra��o das informa��es da mensagem recebida
                String sender = message.getSender();
                String key = message.getKey();
                long timestamp = System.currentTimeMillis();
                String value = message.getValue();

                // Verifica��o do m�todo da mensagem (PUT, GET ou REPLICATION) e execu��o da a��o correspondente
                switch (message.getMethod()) {
                    case "PUT":
                        if (!server.getisLeader()) {
                            sendToLeader(message, out);
                            break;
                        }

                        put(key, value, timestamp, sender, out);
                        break;

                    case "GET":
                        get(key, value, message.getTimestamp(), out, clientSocket);
                        break;

                    case "REPLICATION":
                        replicate(key, value, timestamp, sender, message, out);
                        break;
                }

                // Fechamento do socket do cliente
                clientSocket.close();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // Fun��o para realizar a opera��o PUT no servidor
        private void put(String key, String value, long timestamp, String sender, ObjectOutputStream out) throws IOException {
            // Adiciona a chave e o valor na tabela de armazenamento do servidor
            keyValueStorageTable.put(key, new Mensagem("PUT", key, value, timestamp, sender));

            // Registra a replica��o da opera��o PUT nos outros servidores
            if (registryReplication(key, value, timestamp, sender))
                out.writeObject(new Mensagem("PUT_OK", key, value, timestamp, ip + ":" + port));

            System.out.println(createMessagePUT(key, value, clientAddres(clientSocket)));
        }

        // Fun��o para realizar a opera��o GET no servidor
        private void get(String key, String value, long timestamp, ObjectOutputStream out, Socket socket) throws IOException {
            long timestampServer = -1L;

            if (server.keyValueStorageTable.containsKey(key)) {
                Mensagem message = server.keyValueStorageTable.get(key);
                timestampServer = message.getTimestamp();

                // Verifica se o valor associado � chave � mais recente que o timestamp informado pelo cliente
                value = timestampServer >= timestamp ? message.getValue() : "TRY_OTHER_SERVER_OR_LATER";

            } else {
                value = "Chave nao encontrada ";
            }

            // Envia a resposta da opera��o GET para o cliente
            out.writeObject(new Mensagem("GET_RESPONSE", key, value, timestampServer, server.ip + ":" + port));

            System.out.println(createMessageGET(key, value, timestamp, timestampServer));
        }

        // Fun��o para replicar a opera��o PUT nos outros servidores
        private void replicate(String key, String value, long timestamp, String sender, Mensagem message, ObjectOutputStream out) throws IOException {
            keyValueStorageTable.put(key, new Mensagem("PUT", key, value, timestamp, sender));
            System.out.println(createMessageREPLICATION(key, value, message));
            out.writeObject(new Mensagem("REPLICATION_OK", key, value, System.currentTimeMillis(), ip + ":" + port));
        }

        // Fun��o para registrar a replica��o da opera��o PUT nos outros servidores
        private boolean registryReplication(String key, String value, long timestamp, String sender) {
            for (int port : serverPorts) {
                try (Socket serverSocket = new Socket(ipLocal, port);
                     ObjectOutputStream out = new ObjectOutputStream(serverSocket.getOutputStream());
                     ObjectInputStream in = new ObjectInputStream(serverSocket.getInputStream())) {

                    // Aguarda 10 segundos antes de enviar a mensagem de replica��o
                    Thread.sleep(10000);

                    out.writeObject(new Mensagem("REPLICATION", key, value, timestamp, sender));
                    Mensagem message = (Mensagem) in.readObject();

                    if (!message.getMethod().equals("REPLICATION_OK") || message.equals(null)) return false;

                    serverSocket.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            }
            return true;
        }

        // Fun��o para enviar a requisi��o PUT para o l�der
        private void sendToLeader(Mensagem message, ObjectOutputStream outCliente) {
            try (Socket socket = new Socket(ipLeader, portLeader)) {
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                out.writeObject(message);
                System.out.println("Encaminhando PUT key:[" + message.getKey() + "] value:[" + message.getValue() + "]");

                outCliente.writeObject((Mensagem) in.readObject());
                socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // Fun��o para obter o endere�o do cliente conectado
        private String clientAddres(Socket clientSocket) {
            return clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getLocalPort();
        }

        // Fun��es para criar mensagen da opera��o PUT
        private String createMessagePUT(String key, String value, String address) {
            return "Cliente [" + address + "] PUT key:[" + key + "] value:[" + value + "].";
        }

        // Fun��es para criar mensagen da opera��o GET
        private String createMessageGET(String key, String value, long timestamp, long timestampServer) {
            return "Cliente [" + clientAddres(clientSocket) + "] GET key:[" + key + "] ts:[" + timestamp
                    + "]. Meu ts � [" + timestampServer + "], portanto devolvendo " + "valor: " + value + ".";
        }

        // Fun��es para criar mensagen da opera��o REPLICATION
        private String createMessageREPLICATION(String key, String value, Mensagem message) {
            return "REPLICATION key:[" + key + "] value:[" + value + "] ts:[" + message.getTimestamp() + "].";
        }
    }

    public static void main(String[] args) {
        // Leitura das informa��es do servidor e do l�der fornecidas pelo usu�rio
        System.out.print("IP do servidor atual: ");
        String serverIP = scanner.nextLine().trim();
        serverIP = serverIP.isEmpty() ? ipLocal : serverIP;

        Integer serverport = receiveServerPort();

        System.out.print("IP do servidor L�der: ");
        String serveripLeader = scanner.nextLine().trim();
        serveripLeader = serveripLeader.isEmpty() ? ipLocal : serveripLeader;

        System.out.print("Porta do servidor L�der: ");
        int portLeader = Integer.parseInt(scanner.nextLine());

        // Instancia��o do servidor e configura��o do status de l�der
        Servidor servidor = new Servidor(serverIP, serverport, serveripLeader, portLeader, getPortValues(portLeader));

        servidor.setIsLeader(serverIP.equals(serveripLeader) && serverport.equals(portLeader));

        // Inicializa��o do servidor
        servidor.initialize();
    }
}
