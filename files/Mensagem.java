import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

// Classe Mensagem representa uma mensagem que pode ser enviada entre os componentes do sistema distribuído.
//A classe implementa a interface Serializable para permitir que os objetos dessa classe sejam convertidos em bytes para serem transmitidos pela rede.
public class Mensagem implements Serializable {
    private String method;
    private String key;
    private String value;
    private Long timestamp;
    private String sender;

    public Mensagem(String method, String key, String value, Long timestamp, String sender) {
        this.method = method;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.sender = sender;
    }
    
    // Métodos getters e setters para os atributos da classe Mensagem.

    public String getMethod() {
        return method;
    }

    public void setMethod(String metodo) {
        this.method = metodo;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getSender() {
        return sender;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

}