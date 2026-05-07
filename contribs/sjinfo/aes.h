/*****************************************************************************\
 *  aes.h - implementation-independent job of influxdb info
 *  functions
 *****************************************************************************/
/*****************************************************************************\
 *  Modification history
 *  
\*****************************************************************************/
#ifndef AES_H_INCLUDED
#define AES_H_INCLUDED
#include <stdint.h>
#include <stdio.h>
#include <sys/types.h>

typedef struct{
    uint32_t eK[44], dK[44];    // encKey, decKey
    int Nr; // 10 rounds
} AesKey;

extern const uint8_t S[256];
extern const uint8_t RES_S[256];

/*AES-128 packet length is 16 bytes*/
#define BLOCKSIZE 16
#define LOAD32H(x, y) \
  do { (x) = ((uint32_t)((y)[0] & 0xff)<<24) | ((uint32_t)((y)[1] & 0xff)<<16) | \
             ((uint32_t)((y)[2] & 0xff)<<8)  | ((uint32_t)((y)[3] & 0xff));} while(0)


#define STORE32H(x, y) \
  do { (y)[0] = (uint8_t)(((x)>>24) & 0xff); (y)[1] = (uint8_t)(((x)>>16) & 0xff);   \
       (y)[2] = (uint8_t)(((x)>>8) & 0xff); (y)[3] = (uint8_t)((x) & 0xff); } while(0)

/*Extract the nth byte starting from the low bit from uint32_t x*/ 
#define BYTE(x, n) (((x) >> (8 * (n))) & 0xff)

/*
 *used for key_data_encryption
 *Byte replacement then rotate left by 1 bit
 */

#define ROF32(x, n)  (((x) << (n)) | ((x) >> (32-(n))))

#define ROR32(x, n)  (((x) >> (n)) | ((x) << (32-(n))))

#define MAX_STRING_LENGTH 2000
/* for 128-bit blocks, Rijndael never uses more than 10 rcon values */

static const uint32_t rcon[10] = {
        0x01000000UL, 0x02000000UL, 0x04000000UL, 0x08000000UL, 0x10000000UL,
        0x20000000UL, 0x40000000UL, 0x80000000UL, 0x1B000000UL, 0x36000000UL
};


int load_state_array(uint8_t (*state)[4], const uint8_t *in);
int store_array(uint8_t (*state)[4], uint8_t *out);
int key_data_encryption(const uint8_t *key, uint32_t keyLen, AesKey *aesKey);
int round_key_add(uint8_t (*state)[4], const uint32_t *key);
/*Byte replacement*/
int apply_sbox(uint8_t (*state)[4]);
/*reverse byte replacement*/
int apply_inv_sbox(uint8_t (*state)[4]);
/*row shift*/
int rotate_rows(uint8_t (*state)[4]);
/*retrograde shift*/
int reverse_shift_rows(uint8_t (*state)[4]);
/* Galois Field (256) Multiplication of two Bytes
 * Two-byte Galois field multiplication
 */
uint8_t galois_multiply(uint8_t u, uint8_t v);
/*column mix*/
int mix_cols(uint8_t (*state)[4]);
/*reverse column mix*/
int inv_column_mix(uint8_t (*state)[4]);
/*
*AES-128 encryption interface, the input key should be 16 bytes in length, and the output length should be an integral multiple of 16 bytes.
*In this way, the output length is the same as the input length, and the function calls external memory to allocate memory for the output data.
*/
int encrypt_aes(const uint8_t *key, uint32_t keyLen, const uint8_t *pt, uint8_t *ct, uint32_t len);
/*AES128 decryption, parameter requirements are the same as encryption*/
extern int decrypt_aes(const uint8_t *key, uint32_t keyLen, const uint8_t *ct, uint8_t *pt, uint32_t len);
/*Conveniently output hexadecimal data*/ 
void print_hex(uint8_t *ptr, int len, char *tag);

#endif /* AES_H_INCLUDED */