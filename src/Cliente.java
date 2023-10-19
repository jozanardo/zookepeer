import java.io.*;
import java.net.Socket;
import java.util.*;

public class Cliente {
	
    private final String ip = "127.0.0.1";
    private final int[] serverPorts = {10097, 10098, 10099};
    private static List<String> serverIPsList = new ArrayList<>();
    private static List<Integer> serverPortsList = new ArrayList<>();
    private static Map<String, Long> timeStamp = new HashMap<>();

    private Random random = new Random();
    private static Scanner scanner = new Scanner(System.in);

    // Função para inicializar o cliente e interagir com o servidor
    public void inicializar() {

        // Coleta os IPs e portas dos servidores através da entrada do usuário
        for (int i = 0; i < 3; i++) {
            System.out.print("Digite o IP do servidor [" + (i + 1) + "]: ");
            String serverIP = scanner.nextLine().trim();
            serverIP = serverIP.isEmpty() ? ip : serverIP;
            serverIPsList.add(serverIP);

            System.out.print("Digite a porta do servidor [" + (i + 1) + "]: ");
            String serverPort = scanner.nextLine().trim();

            serverPortsList.add(serverPort.isEmpty() ? serverPorts[i] : Integer.parseInt(serverPort));
        }

        // Loop para mostrar o menu e processar os comandos do cliente
        while (true) {
            System.out.println("-------MENU-------");
            System.out.println("[0] Sair");
            System.out.println("[1] PUT");
            System.out.println("[2] GET");
            System.out.print("Digite um numero: ");

            int command = Integer.parseInt(scanner.nextLine());

            // Processa o comando escolhido pelo cliente
            switch (command) {
                case 0:
                    System.exit(0);
                case 1:
                    System.out.print("Digite a key: ");
                    String key_put = scanner.nextLine();

                    System.out.print("Digite o value: ");
                    String value = scanner.nextLine();

                    // Inicia uma nova thread para enviar a requisição PUT ao servidor escolhido aleatoriamente
                    Thread threadPut = new Thread(() -> sendPut(key_put, value, random.nextInt(serverIPsList.size())));
                    threadPut.start();
                    break;

                case 2:
                    System.out.print("Digite a key: ");
                    String key_get = scanner.nextLine();

                    // Inicia uma nova thread para enviar a requisição GET ao servidor escolhido aleatoriamente
                    Thread threadGet = new Thread(() -> sendGet(key_get, random.nextInt(serverIPsList.size())));
                    threadGet.start();
                    break;

                default:
                    System.out.println("Número digitado inválido. Tente novamente");
                    break;
            }
        }
    }

    // Função para enviar uma requisição PUT para o servidor especificado pelo índice
    private static void sendPut(String key, String value, int indexServer) {
        try (Socket clientSocket = connectToServer(indexServer)) {
            ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());

            // Envia a mensagem de PUT contendo a chave, valor e remetente
            out.writeObject(new Mensagem("PUT", key, value, 0L, clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort()));

            // Recebe a resposta do servidor
            Mensagem response = (Mensagem) in.readObject();
            if (response.getMethod().equals("PUT_OK")) {
                // Atualiza o timestamp da chave com o valor recebido do servidor
                timeStamp.put(key, response.getTimestamp());
                System.out.println(messagePUT_OK(response));
            } else {
                System.out.println("Erro ao realizar o PUT");
            }

            clientSocket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Função para formatar a mensagem de resposta PUT_OK
    private static String messagePUT_OK(Mensagem message) {
        return "PUT_OK key: "
                + message.getKey()
                + " value "
                + message.getValue()
                + " timestamp "
                + message.getTimestamp()
                + " realizada no servidor ["
                + message.getSender().split(":")[0]
                + ":" + message.getSender().split(":")[1] + "]";
    }

    // Função para enviar uma requisição GET para o servidor especificado pelo índice
    private static void sendGet(String key, int indexServer) {
        try (Socket clientSocket = connectToServer(indexServer)) {
            ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());

            long timestemp = timeStamp.getOrDefault(key, 0L);

            // Envia a mensagem de GET contendo a chave e o timestamp
            out.writeObject(new Mensagem("GET", key, null, timestemp, clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort()));

            // Recebe a resposta do servidor
            Mensagem response = (Mensagem) in.readObject();

            if (response.getMethod().equals("GET_RESPONSE") && response.getValue() != null) {
                // Atualiza o timestamp da chave com o valor recebido do servidor
                timeStamp.put(response.getKey(), response.getTimestamp());

                // Formata a mensagem de resposta do GET
                System.out.println(messageGET_OK(response, key, timestemp, indexServer));

            } else {
                System.out.println("Chave " + key + " não encontrada");
            }

            clientSocket.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Função para formatar a mensagem de resposta do GET
    private static String messageGET_OK(Mensagem message, String key, long timestemp, int indexServer) {
        return "GET key: "
                + key
                + " value: "
                + message.getValue()
                + " obtido do servidor "
                + serverIPsList.get(indexServer)
                + ":" + serverPortsList.get(indexServer)
                + ", meu timestamp "
                + timestemp
                + " e do servidor "
                + message.getTimestamp();
    }

    // Função para estabelecer a conexão com o servidor especificado pelo índice
    private static Socket connectToServer(int indexServer) throws IOException {
        return new Socket(serverIPsList.get(indexServer), serverPortsList.get(indexServer));
    }

    public static void main(String[] args) {
        Cliente cliente = new Cliente();
        cliente.inicializar();
    }
}