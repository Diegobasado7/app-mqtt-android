package com.example.app_mqtt_android;

import android.os.Bundle;
import android.util.Log;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import androidx.appcompat.app.AppCompatActivity;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MainActivity extends AppCompatActivity {

    private static final String BROKER = "tcp://test.mosquitto.org:1883";
    private static final String TOPIC = "test/topic"; // mejor uno propio, ej. "tunombre/test"

    private MqttClient client;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private TextView tvEstado;
    private TextView tvMensaje;
    private EditText etMensaje;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        tvEstado = findViewById(R.id.tvEstado);
        tvMensaje = findViewById(R.id.tvMensaje);
        etMensaje = findViewById(R.id.etMensaje);
        Button btnPublicar = findViewById(R.id.btnPublicar);

        conectar();

        btnPublicar.setOnClickListener(v -> {
            String texto = etMensaje.getText().toString().trim();
            if (!texto.isEmpty()) {
                publicar(texto);
                etMensaje.setText("");
            }
        });
    }

    private void conectar() {
        // La red no puede usarse en el hilo principal, por eso va en un executor
        executor.execute(() -> {
            try {
                String clientId = MqttClient.generateClientId();
                client = new MqttClient(BROKER, clientId, new MemoryPersistence());

                client.setCallback(new MqttCallback() {
                    @Override
                    public void connectionLost(Throwable cause) {
                        Log.e("MQTT", "Conexión perdida", cause);
                        runOnUiThread(() -> tvEstado.setText("Estado: conexión perdida"));
                    }

                    @Override
                    public void messageArrived(String topic, MqttMessage message) {
                        String texto = new String(message.getPayload());
                        Log.d("MQTT", "Mensaje en " + topic + ": " + texto);
                        runOnUiThread(() -> tvMensaje.setText(texto));
                    }

                    @Override
                    public void deliveryComplete(IMqttDeliveryToken token) {
                        Log.d("MQTT", "Mensaje entregado");
                    }
                });

                MqttConnectOptions options = new MqttConnectOptions();
                options.setCleanSession(true);
                options.setAutomaticReconnect(true);

                client.connect(options);
                Log.d("MQTT", "Conectado");
                runOnUiThread(() -> tvEstado.setText("Estado: conectado"));

                client.subscribe(TOPIC, 0);
                Log.d("MQTT", "Suscrito a " + TOPIC);

            } catch (MqttException e) {
                Log.e("MQTT", "Fallo al conectar", e);
                runOnUiThread(() -> tvEstado.setText("Estado: error al conectar"));
            }
        });
    }

    private void publicar(String texto) {
        executor.execute(() -> {
            try {
                if (client != null && client.isConnected()) {
                    MqttMessage mensaje = new MqttMessage(texto.getBytes());
                    mensaje.setQos(0);
                    client.publish(TOPIC, mensaje);
                } else {
                    Log.w("MQTT", "No conectado, no se puede publicar");
                }
            } catch (MqttException e) {
                Log.e("MQTT", "Error al publicar", e);
            }
        });
    }

    @Override
    protected void onDestroy() {
        super.onDestroy();
        executor.execute(() -> {
            try {
                if (client != null) {
                    client.disconnect();
                    client.close();
                }
            } catch (MqttException e) {
                e.printStackTrace();
            }
        });
        executor.shutdown();
    }
}