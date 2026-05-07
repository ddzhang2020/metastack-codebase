/*****************************************************************************\
 *  influxdb_src.c - responsible for InfluxDB data transmission and reception.
 *****************************************************************************
 *  Author: Carlos Fenoy Garcia
 *  Copyright (C) 2016 F. Hoffmann - La Roche
 *
 *  Based on the HDF5 profiling plugin and Elasticsearch job completion plugin.
 *
 *  Portions Copyright (C) 2013 Bull S. A. S.
 *		Bull, Rue Jean Jaures, B.P.68, 78340, Les Clayes-sous-Bois.
 *
 *  Copyright (C) SchedMD LLC.
 *
 *  This file is part of Slurm, a resource management program.
 *  For details, see <http://www.schedmd.com/slurmdocs/>.
 *  Please also read the included file: DISCLAIMER.
 *
 *  Slurm is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  In addition, as a special exception, the copyright holders give permission
 *  to link the code of portions of this program with the OpenSSL library under
 *  certain conditions as described in each individual source file, and
 *  distribute linked combinations including the two. You must obey the GNU
 *  General Public License in all respects for all of the code used other than
 *  OpenSSL. If you modify file(s) with this exception, you may extend this
 *  exception to your version of the file(s), but you are not obligated to do
 *  so. If you do not wish to do so, delete this exception statement from your
 *  version.  If you delete this exception statement from all source files in
 *  the program, then also delete it here.
 *
 *  Slurm is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with Slurm; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
 *
 *  This file is patterned after jobcomp_linux.c, written by Morris Jette and
 *  Copyright (C) 2002 The Regents of the University of California.
 \*****************************************************************************/
#define _GNU_SOURCE
#include <string.h>
#include <strings.h>
#include <stdint.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdbool.h>
#include <curl/curl.h>
#include "influx_src.h" 
#include "xstring2.h" 
#include "time_format.h"
#include "aes.h"

#define HEX_LINE_BUF_SIZE        1000
#define AES_KEY_LEN_BYTES        16
#define AES_BLOCK_LEN_BYTES      32
#define CIPHERTEXT_LINE_COUNT    3
#define METADATA_LINE_COUNT      3
#define METADATA_START_LINE      4
#define MIN_REQUIRED_LINES       6
const char *RPTypeNames[] = {
	"NATIVERP",
    "STEPDRP",
    "EVENTRP",
    "APPTYPERP",
};

uint8_t ct1[32] = {0};    
uint8_t ct2[32] = {0};    
uint8_t ct3[32] = {0};
char    data3[32] = {0};    
uint8_t plain1[32] = {0};
uint8_t plain2[32] = {0}; 
uint8_t plain3[32] = {0}; 

static int _hex_val(char c)
{
	if (c >= '0' && c <= '9')
		return c - '0';
	if (c >= 'a' && c <= 'f')
		return 10 + (c - 'a');
	if (c >= 'A' && c <= 'F')
		return 10 + (c - 'A');
	return -1;
}

static int _hex_decode_line(const char *line, uint8_t **out, size_t *out_len)
{
	size_t len = 0, i = 0, w = 0;
	uint8_t *buf = NULL;

	if (!line || !out || !out_len)
		return SLURM_ERROR;

	/* Measure hex chars up to newline/CR */
	while (line[len] && line[len] != '\n' && line[len] != '\r')
		len++;

	if (len == 0 || (len % 2) != 0)
		return SLURM_ERROR;

	buf = xmalloc(len / 2);
	for (i = 0; i < len; i += 2) {
		int hi = _hex_val(line[i]);
		int lo = _hex_val(line[i + 1]);
		if (hi < 0 || lo < 0) {
			xfree(buf);
			return SLURM_ERROR;
		}
		buf[w++] = (uint8_t)((hi << 4) | lo);
	}

	*out = buf;
	*out_len = w;
	return SLURM_SUCCESS;
}

static void _free_and_set(char **dst, char *val)
{
	if (dst == NULL)
		return;
	if (*dst)
		xfree(*dst);
	*dst = val;
}
/**
 * @brief Parses the runtime policy string and extracts the policy value matching the specified type.
 *
 * This function takes a runtime policy string (rt_policy) and a target policy type (type), and parses the comma-separated
 * components of the policy string. If a matching type is found, it returns the associated value. If no matching policy is found,
 * it returns the default value "autogen". If the input rt_policy is NULL or an empty string, it directly returns "autogen".
 *
 * If the rt_policy string contains a standalone value (without any key-value pairs), it is treated as the default return value.
 * If no matching key is found, but a standalone value exists, the function returns that value.
 * If the string does not contain any valid key-value pairs, it is returned as is.
 *
 * @param rt_policy The runtime policy string, consisting of comma-separated key-value pairs (e.g., "type1=value1,type2=value2")
 *                  or a single default value.
 * @param type The target policy type, used to match a specific policy value. This is an enum representing different policy types.
 *
 * @return A string containing the policy value corresponding to the specified type.
 *         - If a matching key is found, returns its associated value.
 *         - If no matching key is found but a standalone default value exists, returns that value.
 *         - If no valid key-value pairs are found, returns the original rt_policy string.
 *         - If the input is NULL or empty, returns "autogen".
 *         - If at least one key is found but the requested type is missing, returns "autogen".
 */
extern char* _parse_rt_policy(const char *rt_policy, RPType type) {
    int i = 0;
    if (rt_policy == NULL || rt_policy[0] == '\0') {
        return xstrdup("autogen");
    }

    // If rt_policy does not contain ',' or '=', return it directly
    if (strchr(rt_policy, ',') == NULL && strchr(rt_policy, '=') == NULL) {
        return xstrdup(rt_policy);
    }

    int found_any_keyword = 0;
    char *default_value = NULL; 
    char *policy_copy = xstrdup(rt_policy);
    if (!policy_copy) {
        return xstrdup("autogen");
    }

    char *saveptr = NULL;
    char *token = strtok_r(policy_copy, ",", &saveptr);

    while (token) {
        char *value = strchr(token, '=');
        if (value) {
            *value = '\0';  
            value++;        

            for (i = 0; i < RPCNT; i++) {
                if (xstrcmp(token, RPTypeNames[i]) == 0) {
                    found_any_keyword = 1;
                    if (i == (int)type) {
                        char *result = xstrdup(value);
                        xfree(default_value);
                        xfree(policy_copy);
                        return result;
                    }
                }
            }
        } else {
            // If no '=' is found, treat it as the default value
            xfree(default_value);
            default_value = xstrdup(token);
        }
        token = strtok_r(NULL, ",", &saveptr);
    }

    xfree(policy_copy);

    // Return the default value if no match is found
    if (default_value) {
        return default_value;
    }

    // If a key is found but no match for type, return "autogen"
    if (found_any_keyword) {
        return xstrdup("autogen");
    }

    // If no key-value structure is found, return the original string
    return xstrdup(rt_policy);
}

/* Callback to handle the HTTP response */
static size_t write_callback(void *contents, size_t size, size_t nmemb,
			      void *userp)
{
	size_t realsize = size * nmemb;
	struct http_response *mem = (struct http_response *) userp;
	mem->message = xrealloc(mem->message, mem->size + realsize + 1);
	memcpy(&(mem->message[mem->size]), contents, realsize);
	mem->size += realsize;
	mem->message[mem->size] = 0;
	return realsize;
}

extern char* influxdb_connect(slurm_influxdb *data, const char* sql, int type, bool display)
{
    struct http_response chunk;
    CURL *curl;
    CURLcode res;
    if (curl_global_init(CURL_GLOBAL_ALL) != 0) {
        printf("influxdb_connect init curl global all failed \n");
        return NULL;
    } else if ((curl = curl_easy_init()) == NULL) {
        printf("influxdb_connect init curl failed \n");
        return NULL;      
    }

    chunk.message = xmalloc(1);
	chunk.size = 0;
    
    // Initialize libcurl

    if(curl) {
       
        char *url1 = (char*)xmalloc(200);
        char *url2 = (char*)xmalloc(100 + strlen(sql));
        char *policy = _parse_rt_policy(data->policy, type);
        if (display)
            sprintf(url1, "%s/query?db=%s&rp=%s&epoch=s", data->host, data->database, policy);
        else
            sprintf(url1, "%s/query?db=%s&rp=%s&precision=s", data->host, data->database, policy);
        xfree(policy);
        sprintf(url2,"q=%s",sql);
        curl_easy_setopt(curl, CURLOPT_URL, url1);

		curl_easy_setopt(curl, CURLOPT_USERNAME,
				 data->username);

        curl_easy_setopt(curl, CURLOPT_PASSWORD,
				  data->password);

        /*Set timeout to 300 seconds*/
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 300L);

        curl_easy_setopt(curl, CURLOPT_POST, 1);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, url2);


        /* Set callback function to receive response data*/
  
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);

        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &chunk);
 
        /*Perform the HTTP GET request*/
        static int error_cnt = 0;

        res = curl_easy_perform(curl);

        /* Check for errors*/
        if(res != CURLE_OK) {
            fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
            if ((error_cnt++ % 100) == 0)
            printf("curl_easy_perform failed to send data (discarded). Reason %s\n", curl_easy_strerror(res));
        }
        curl_easy_cleanup(curl);
        xfree(url1);
        xfree(url2);
    }
    if(res != CURLE_OK)
        return NULL;
    else
        return chunk.message;
}

extern int _send_data2(slurm_influxdb *influxdb_conf, char *datastr2)
{
	CURL *curl_handle = NULL;
	CURLcode res;
	struct http_response chunk;
	int rc = SLURM_SUCCESS;
	long response_code = 0;
	static int error_cnt = 0;
	char *url = NULL, *policy = NULL;
	//size_t length;
	/*
	 * Every compute node which is sampling data will try to establish a
	 * different connection to the influxdb server. The data will not be 
	 * cached and will be sent in real time at the head node of the job.
	 */
	DEF_TIMERS;
	START_TIMER;

	if (curl_global_init(CURL_GLOBAL_ALL) != 0) {
		printf("curl_easy_init in CURL_GLOBAL_ALL\n");
		rc = SLURM_ERROR;
		goto cleanup_global_init;
	} else if ((curl_handle = curl_easy_init()) == NULL) {
		printf("curl_easy_init in curl_handle\n");
		rc = SLURM_ERROR;
		goto cleanup_easy_init;
	}

    policy = _parse_rt_policy(influxdb_conf->policy, RPCNT);
	xstrfmtcat(url, "%s/write?db=%s&rp=%s&precision=ns", influxdb_conf->host,
		   influxdb_conf->database, policy);
	if(policy)
        xfree(policy);

	chunk.message = xmalloc(1);
	chunk.size = 0;

	curl_easy_setopt(curl_handle, CURLOPT_URL, url);
	if (influxdb_conf->password)
		curl_easy_setopt(curl_handle, CURLOPT_PASSWORD,
				 influxdb_conf->password);

	curl_easy_setopt(curl_handle, CURLOPT_POST, 1);
	curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDS, datastr2);
	curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDSIZE, strlen(datastr2));
	if (influxdb_conf->username)
		curl_easy_setopt(curl_handle, CURLOPT_USERNAME,
				 influxdb_conf->username);
	curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, write_callback);
	curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *) &chunk);

	if ((res = curl_easy_perform(curl_handle)) != CURLE_OK) {
		if ((error_cnt++ % 100) == 0)
			printf("curl_easy_perform failed to send data (discarded). Reason: %s\n",
			                 curl_easy_strerror(res));
		rc = SLURM_ERROR;
		goto cleanup;
	}

	if ((res = curl_easy_getinfo(curl_handle, CURLINFO_RESPONSE_CODE,
				     &response_code)) != CURLE_OK) {
		printf("curl_easy_getinfo response code failed: %s\n",
		                                    curl_easy_strerror(res));
		rc = SLURM_ERROR;
		goto cleanup;
	}

	/* In general, status codes of the form 2xx indicate success,
	 * 4xx indicate that InfluxDB could not understand the request, and
	 * 5xx indicate that the system is overloaded or significantly impaired.
	 * Errors are returned in JSON.
	 * https://docs.influxdata.com/influxdb/v0.13/concepts/api/
	 */
	if (response_code >= 200 && response_code <= 205) {
		printf("data write success \n");
		if (error_cnt > 0)
			error_cnt = 0;
	} else {
		rc = SLURM_ERROR;
		printf("data write failed, response code: %ld \n", response_code);
			/* Strip any trailing newlines. */
			while (chunk.message[strlen(chunk.message) - 1] == '\n')
				chunk.message[strlen(chunk.message) - 1] = '\0';
			printf("JSON response body: %s \n", chunk.message);
	}


cleanup:
	xfree(chunk.message);
	xfree(url);
cleanup_easy_init:
	curl_easy_cleanup(curl_handle);
cleanup_global_init:
	curl_global_cleanup();
	END_TIMER;
	printf("took %s to send data \n", TIME_STR);
	return rc;	
}

/*Read a hexadecimal string and convert it to a uint8_t type array*/
extern int read_hex_bytes_from_file(char *path_tmp, const uint8_t *key, slurm_influxdb *data)
{
	FILE *fp_out = NULL;
	char tmp_str[HEX_LINE_BUF_SIZE] = {0};
	int line_no = 0;
	int rc = SLURM_ERROR;
	bool got_cipher[CIPHERTEXT_LINE_COUNT] = {false, false, false};
	bool got_meta[METADATA_LINE_COUNT] = {false, false, false};
	uint8_t *plain_targets[CIPHERTEXT_LINE_COUNT] = {plain1, plain2, plain3};
	char **meta_targets[METADATA_LINE_COUNT] = {&data->database, &data->host, &data->policy};

	if (path_tmp == NULL || key == NULL || data == NULL) {
		printf("read_hex_bytes_from_file: invalid args\n");
		return SLURM_ERROR;
	}

	/* Clear cached plaintext to avoid stale data from previous runs. */
	memset(plain1, 0, sizeof(plain1));
	memset(plain2, 0, sizeof(plain2));
	memset(plain3, 0, sizeof(plain3));

	fp_out = fopen(path_tmp, "r");
	if (fp_out == NULL) {
		printf("Failed to open file: %s\n", path_tmp);
		return SLURM_ERROR;
	}

	while (fgets(tmp_str, sizeof(tmp_str), fp_out) != NULL) {
		uint8_t *bytes = NULL;
		size_t bytes_len = 0;
		int idx = 0;

		/* skip blank lines */
		if (tmp_str[0] == '\n' || tmp_str[0] == '\r' || tmp_str[0] == '\0')
			continue;

		line_no++;
		if (_hex_decode_line(tmp_str, &bytes, &bytes_len) != SLURM_SUCCESS) {
			printf("Invalid hex data at line %d in %s\n", line_no, path_tmp);
			goto cleanup;
		}

		if (line_no >= 1 && line_no <= CIPHERTEXT_LINE_COUNT) {
			if (bytes_len != AES_BLOCK_LEN_BYTES) {
				printf("Invalid ciphertext length at line %d (got %zu, want 32)\n",
				       line_no, bytes_len);
				goto cleanup_line;
			}
			idx = line_no - 1;
			decrypt_aes(key, AES_KEY_LEN_BYTES, bytes, plain_targets[idx], AES_BLOCK_LEN_BYTES);
			got_cipher[idx] = true;

			goto cleanup_line;
		}

		/* Lines 4-6 are database/host/policy as hex-encoded ASCII (no terminator in file). */
		if (line_no >= METADATA_START_LINE && line_no <= MIN_REQUIRED_LINES) {
			char *s = xmalloc(bytes_len + 1);
			memcpy(s, bytes, bytes_len);
			s[bytes_len] = '\0';
			idx = line_no - METADATA_START_LINE;
			_free_and_set(meta_targets[idx], s);
			got_meta[idx] = true;
			goto cleanup_line;
		}

		/* Ignore any extra lines (future extension). */
cleanup_line:
		xfree(bytes);
	}

	/* Require all current mandatory lines to be present. */
	for (int i = 0; i < CIPHERTEXT_LINE_COUNT; i++) {
		if (!got_cipher[i]) {
			printf("Missing ciphertext line %d in %s\n", i + 1, path_tmp);
			goto cleanup;
		}
		if (!got_meta[i]) {
			printf("Missing metadata line %d in %s\n", i + METADATA_START_LINE, path_tmp);
			goto cleanup;
		}
	}

	/* Parse username/password lengths from decrypted plain3: "len1:len2" padded with '#'. */
	{
		char lenbuf[AES_BLOCK_LEN_BYTES + 1] = {0};
		int ulen = 0, plen = 0;
		int parsed = 0;

		memcpy(lenbuf, plain3, AES_BLOCK_LEN_BYTES);
		lenbuf[AES_BLOCK_LEN_BYTES] = '\0';

		/* stop at first padding char */
		for (size_t i = 0; i < AES_BLOCK_LEN_BYTES; i++) {
			if (lenbuf[i] == '#') {
				lenbuf[i] = '\0';
				break;
			}
		}

		parsed = sscanf(lenbuf, "%d:%d", &ulen, &plen);
		if (parsed != 2 || ulen < 0 || ulen > AES_BLOCK_LEN_BYTES ||
		    plen < 0 || plen > AES_BLOCK_LEN_BYTES) {
			printf("Invalid decrypted length header: '%s'\n", lenbuf);
			goto cleanup;
		}

		/* Copy exact lengths from decrypted padded buffers. */
		{
			char *u = xmalloc((size_t)ulen + 1);
			char *p = xmalloc((size_t)plen + 1);

			memcpy(u, plain1, (size_t)ulen);
			u[ulen] = '\0';
			memcpy(p, plain2, (size_t)plen);
			p[plen] = '\0';

			_free_and_set(&data->username, u);
			_free_and_set(&data->password, p);
		}
	}

	rc = SLURM_SUCCESS;

cleanup:
	if (fp_out)
		fclose(fp_out);
	return rc;
}