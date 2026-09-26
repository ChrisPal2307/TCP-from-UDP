#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h>

// Tamaños de buffer y payload
#define CHUNK_SIZE 1024             // Tamaño máximo de datos por paquete (1 KB)
#define MAX_FILENAME_LEN 256        // Tamaño máximo para el nombre del archivo

// Tipos de paquetes (Banderas/Flags)
typedef enum {
    TYPE_SYN = 1,   // Inicio de conexión / Envío de metadatos (nombre de archivo, tamaño)
    TYPE_DATA = 2,  // Paquete con fragmento de datos del archivo
    TYPE_ACK = 3,   // Acuse de recibo / Confirmación
    TYPE_FIN = 4    // Fin de transmisión
} PacketType;

/* 
 * Estructura del Paquete Principal (Emisor -> Receptor)
 */
typedef struct __attribute__((packed)) {
    uint8_t  type;               // Tipo de paquete (SYN, DATA, ACK, FIN)
    uint32_t seq_num;            // Número de secuencia del paquete
    uint32_t file_id;            // Identificador de emisor/archivo (para soportar múltiples emisores)
    uint16_t length;             // Longitud de los datos útiles presentes en 'data'
    char     data[CHUNK_SIZE];   // Carga útil (Payload)
} Packet;

/* 
 * Estructura del Acuse de Recibo (Receptor -> Emisor)
 */
typedef struct __attribute__((packed)) {
    uint8_t  type;               // TYPE_ACK
    uint32_t ack_num;            // Número de secuencia que se está confirmando
    uint32_t file_id;            // Para indicar a qué emisor/archivo corresponde la respuesta
} AckPacket;

#endif
