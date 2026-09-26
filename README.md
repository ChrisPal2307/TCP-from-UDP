
## Descripción General

El sistema está compuesto por dos programas:

- **`receptor`**: Servidor que escucha en un puerto local y recibe múltiples transmisiones concurrentes, guardando cada archivo en un hilo dedicado e identificado por un `file id` único.
- **`emisor`**: Cliente que envía un archivo a un receptor remoto, asociándolo a un identificador de sesión (`file id`) para que el receptor lo distinga del resto.

El protocolo implementado maneja pérdida de paquetes, duplicados y retardo, garantizando la reconstrucción fiel y ordenada del archivo original.

---

## Escenario de Prueba y Ejecución

### 1. Inicio del receptor

Se ejecutó el receptor escuchando en el puerto local `8080`:

```bash
./receptor 8080
```
### 2. Inicio de emisiones recurrentes

Se lanzaron dos instancias del emisor de forma simultánea, cada una transmitiendo un archivo distinto:
```bash
./emisor localhost 8080 archivo_grande1.bin 1 &
./emisor localhost 8080 archivo_grande2.bin 2 &
```

### 3. Simulacion de red no confiable

Para simular un entorno de red no confiable  sobre la interfaz de loopback, se aplicó una regla con Traffic Control (tc) y el módulo netem:

```bash
sudo tc qdisc add dev lo root netem delay 1ms loss 5% duplicate 5%
```

---
## Comprobación de Integridad

Finalizadas ambas transmisiones, se comparó byte por byte cada archivo original con el archivo reconstruido por el receptor:
```bash
diff archivo_grande1.bin received_id1_archivo_grande1.bin
diff archivo_grande2.bin received_id2_archivo_grande2.bin
```
