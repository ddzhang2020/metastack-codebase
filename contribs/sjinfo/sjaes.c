/*****************************************************************************\
 *  sjaes.c - implementation-independent job of influxdb info
 *  functions
 *****************************************************************************/
/*****************************************************************************\
 *  Modification history
 *  
\*****************************************************************************/
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <ctype.h>
#include <unistd.h>
#include <sys/stat.h>
#include "sjaes.h"
#include "aes.h"
#include "xstring2.h"
#include "influx_src.h" 
#include "time_format.h"
uint8_t ct1[32] = {0};    //外部申请输出数据内存，用于存放加密后数据
uint8_t plain1[32] = {0}; //外部申请输出数据内存，用于存放解密后数据
uint8_t ct2[32] = {0};    //外部申请输出数据内存，用于存放加密后数据
uint8_t plain2[32] = {0}; //外部申请输出数据内存，用于存放解密后数据

uint8_t ct3[32] = {0};    //外部申请输出数据内存，用于存放加密后数据
uint8_t plain3[32] = {0}; //外部申请输出数据内存，用于存放解密后数据
char data3[64] = {0};

static bool _extract_conf_value(char *line, const char *key, char **dest)
{
    size_t key_len = strlen(key);
    char *value = NULL;

    if (!line || !key || !dest)
        return false;
    if (xstrncmp(line, key, key_len) != 0)
        return false;

    value = line + key_len;
    value[strcspn(value, "\r\n")] = '\0';

    xfree(*dest);
    *dest = xstrdup(value);
    return true;
}

void extract_user_pass(char *filename, slurm_influxdb *influxdb_data)
{
    FILE *file = NULL;
    char buffer[MAX_STRING_LENGTH];
    char *line = NULL;

    if (!filename || !influxdb_data)
        return;

    file = fopen(filename, "r");
    if (!file) {
        printf("Failed to open file %s\n", filename);
        return;
    }

    while (fgets(buffer, sizeof(buffer), file)) {
        line = buffer;
        while (isspace((unsigned char)*line))
            line++;

        if (*line == '#' || *line == '\0')
            continue;

        if (_extract_conf_value(line, "ProfileInfluxDBUser=", &influxdb_data->username))
            continue;
        if (_extract_conf_value(line, "ProfileInfluxDBPass=", &influxdb_data->password))
            continue;
        if (_extract_conf_value(line, "ProfileInfluxDBDatabase=", &influxdb_data->database))
            continue;
        if (_extract_conf_value(line, "ProfileInfluxDBRTPolicy=", &influxdb_data->policy))
            continue;
        if (_extract_conf_value(line, "ProfileInfluxDBHost=", &influxdb_data->host))
            continue;
    }

    fclose(file);
}

void pad_string(char *str, int desiredLength) {

    int currentLength = strlen(str);
    if (currentLength >= desiredLength) {
        // 如果字符串长度已经大于等于指定长度，则不需要填充
        return;
    }
 
    // 计算需要填充的数量
    int paddingLength = desiredLength - currentLength;
    
    // 填充字符串
    //printf("paddingLength=%d str=%s \n",paddingLength,str);
    int i = 0;
    for (i = 0; i < paddingLength-1; ++i) {
        str[currentLength + i] = '#';
       
    }
    // 添加字符串结尾的空字符
    str[currentLength + paddingLength-1] = '\0';
    
}

// 读取十六进制字符串并转换为 uint8_t 类型数组
/*Read a hexadecimal string and convert it to a uint8_t type array*/
static int _hex_char_to_val(char c)
{
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
    if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;
    return -1;
}

static int _decode_hex_line(const char *line, uint8_t **out, size_t *out_len)
{
    size_t len = 0;
    size_t i = 0;
    uint8_t *buf = NULL;

    if (!line || !out || !out_len)
        return -1;

    len = strcspn(line, "\r\n");
    if (len == 0 || (len % 2) != 0)
        return -1;

    buf = (uint8_t *)xmalloc(len / 2);
    for (i = 0; i < len; i += 2) {
        int hi = _hex_char_to_val(line[i]);
        int lo = _hex_char_to_val(line[i + 1]);
        if (hi < 0 || lo < 0) {
            xfree(buf);
            return -1;
        }
        buf[i / 2] = (uint8_t)((hi << 4) | lo);
    }

    *out = buf;
    *out_len = len / 2;
    return 0;
}

int read_hex_bytes_from_file_test(char *path_tmp, const uint8_t *key)
{
	FILE *fp_out = NULL;
    char tmp_str[1000] = {0};
    int line_no = 0;
    int rc = -1;
    uint8_t *plain_targets[3] = {plain1, plain2, plain3};
    char *influxdb[3] = {NULL, NULL, NULL};

    if (path_tmp == NULL || key == NULL) {
        return -1;
    }

	fp_out = fopen(path_tmp, "r");
	if (fp_out == NULL)
        return -1;

    memset(plain1, 0, sizeof(plain1));
    memset(plain2, 0, sizeof(plain2));
    memset(plain3, 0, sizeof(plain3));

    while (fgets(tmp_str, sizeof(tmp_str), fp_out) != NULL) {
        uint8_t *bytes = NULL;
        size_t bytes_len = 0;

        if (tmp_str[0] == '\n' || tmp_str[0] == '\r' || tmp_str[0] == '\0')
            continue;

        line_no++;
        if (_decode_hex_line(tmp_str, &bytes, &bytes_len) != 0)
            goto cleanup;

        if (line_no >= 1 && line_no <= 3) {
            if (bytes_len != 32) {
                xfree(bytes);
                goto cleanup;
            }
            decrypt_aes(key, 16, bytes, plain_targets[line_no - 1], 32);
            printf("plain%d=%s \n", line_no, plain_targets[line_no - 1]);
        } else if (line_no >= 4 && line_no <= 6) {
            size_t idx = (size_t)(line_no - 4);
            influxdb[idx] = (char *)xmalloc(bytes_len + 1);
            memcpy(influxdb[idx], bytes, bytes_len);
            influxdb[idx][bytes_len] = '\0';
        }

        xfree(bytes);
    }

    if (line_no < 3)
        goto cleanup;

    rc = 0;
cleanup:
    if (fp_out)
        fclose(fp_out);
    xfree(influxdb[0]);
    xfree(influxdb[1]);
    xfree(influxdb[2]);
    return rc;
}

// 将十六进制字符串转换为字节流
// 写入字节到文件（以十六进制形式）
void write_hex_bytes_to_file(char* tmp_path, char *configpath, slurm_influxdb* influxdb_data) 
{

    size_t i = 0;
    FILE *sys_file = NULL;
    sys_file = fopen(tmp_path, "w+");
    if (sys_file == NULL) {
        exit(1);
    }

    if (sys_file != NULL) {
        for (i = 0; i < 32; i++) {
            fprintf(sys_file, "%.2X", ct1[i]);  
        }
        fprintf(sys_file, "\n");
        for ( i = 0; i <32; i++) {
            fprintf(sys_file, "%02X", ct2[i]);
        }
        fprintf(sys_file, "\n");
        for ( i = 0; i < 32; i++) {
            fprintf(sys_file, "%02X", ct3[i]);
        }
        fprintf(sys_file, "\n");
        
        if((strlen(influxdb_data->database)>0) && (strlen(influxdb_data->host)>0) && (strlen(influxdb_data->policy)>0)) {
            for ( i = 0; i < strlen(influxdb_data->database); i++) {
                fprintf(sys_file, "%02X", influxdb_data->database[i]);
            }
            fprintf(sys_file, "\n");
            for ( i = 0; i < strlen(influxdb_data->host); i++) {
                fprintf(sys_file, "%02X", influxdb_data->host[i]);
            }
            fprintf(sys_file, "\n");
            for ( i = 0; i < strlen(influxdb_data->policy); i++) {
                fprintf(sys_file, "%02X", influxdb_data->policy[i]);
            }
            fprintf(sys_file, "\n");           
        } 

        fclose(sys_file);
        printf("Hex data written to file: %s\n locate acct_gather.conf.key %s \n", tmp_path, configpath);
        mode_t mode = 0644; // rw-r--r--
        if (chmod(tmp_path, mode) == -1) {
             // 尝试删除文件
            printf("Failed to change file permissions for %s to 644.\n", tmp_path);
            if (remove(tmp_path) == -1) {
                printf("Remove acct_gather.conf failed"); // 输出删除文件时的错误信息
            } else {
                printf("File %s has been deleted. please try again\n", tmp_path);
            }
        }
    } else {
        printf("Failed to open file: %s\n", tmp_path);
    }
}

// 读取十六进制字符串并转换为 uint8_t 类型数组

int main() {

    slurm_influxdb* influxdb_data = NULL;
    if (geteuid() != 0) {
        printf("The sjaes command must be executed as the root user.\n");
        return 0;
     }
    influxdb_data = xmalloc(sizeof(slurm_influxdb));
    char *configpath = NULL;
    char tmp_conf[] = "/etc/acct_gather.conf";

    /*获取slurm.conf 所在etc的位置*/
    if(strcmp(KEYDIR, "NONE") == 0) {
         configpath = xstrdup("/etc/slurm/acct_gather.conf");
    } else {
        char* def_conf = NULL;
        def_conf = xmalloc(strlen(KEYDIR)+strlen(tmp_conf)+2);
        sprintf(def_conf, "%s%s",KEYDIR,tmp_conf);
        configpath = xstrdup(def_conf);
        xfree(def_conf);
    }

    if(!(access(configpath, F_OK) != -1)) {
        printf("The file acct_gather.conf path does not exist in %s. please retry \n",configpath);
        exit(0);
    }
    
    extract_user_pass(configpath, influxdb_data);
    if (influxdb_data->username != NULL && influxdb_data->password != NULL && influxdb_data->policy != NULL) {
        printf("ProfileInfluxDBUser: %s\n", influxdb_data->username);
        printf("ProfileInfluxDBPass: %s\n", influxdb_data->password);
        printf("ProfileInfluxDBRTPolicy: %s\n", influxdb_data->policy);
    } else {
        printf("Failed to read username、password or policy.\n");
        exit(1);
    } 

    /*16位加密key*/
    const uint8_t key[]="fcad715bd73b5cb0";

    /*加密32字节明文*/
    sprintf(data3, "%ld:%ld", strlen(influxdb_data->username), strlen(influxdb_data->password));

    /*提取用户名及密码*/
    if((strlen(influxdb_data->username) > 31) || (strlen(influxdb_data->password) > 31)) {
        printf("error: Usernames or secrets exceeding 31 characters cannot be encrypted. (influxdb_data.username) length=%zu (influxdb_data.password)=%zu \n",
                            strlen(influxdb_data->username), strlen(influxdb_data->password));
        exit(1);
    }

    influxdb_data->username = (char *)xrealloc(influxdb_data->username, 32);
    influxdb_data->password = (char *)xrealloc(influxdb_data->password, 32);
    pad_string(influxdb_data->username, 32);
    pad_string(influxdb_data->password, 32);
    pad_string(data3, 32);

    encrypt_aes(key, 16, (unsigned char *)influxdb_data->username, ct1, 32); 
    encrypt_aes(key, 16, (unsigned char *)influxdb_data->password, ct2, 32); 
    encrypt_aes(key, 16, (unsigned char *)data3, ct3, 32); 


    print_hex(ct1, 32, "after encryption:");
    print_hex(ct2, 32, "after encryption:");
    print_hex(ct3, 32, "after encryption:");
    
    char str2[] = ".key";
    char *tmp_path = xmalloc(strlen(str2)+strlen(configpath)+1);
    strcpy(tmp_path, configpath);
    strcat(tmp_path, str2);
    /*生成key文件*/
    write_hex_bytes_to_file(tmp_path, configpath, influxdb_data);
    
    /*密码读取测试*/
    //read_hex_bytes_from_file(tmp_path, key);
    read_hex_bytes_from_file_test(tmp_path, key);

    /*内存释放*/
    xfree(configpath);
    xfree(influxdb_data->username);
    xfree(influxdb_data->password);
    xfree(influxdb_data->host);
    xfree(influxdb_data->database);
    xfree(influxdb_data->policy);
    xfree(influxdb_data);
    xfree(tmp_path);
    return 0;
}
