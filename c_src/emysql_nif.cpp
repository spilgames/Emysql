#include "erl_nif.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

static ERL_NIF_TERM decode_row_data(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{

	const int max=100;
	int term_ct=0;
	ERL_NIF_TERM Terms[max];

	ErlNifBinary bytes;

	enif_inspect_binary(env, argv[0], &bytes);


	for( uint64_t p = 0; p < bytes.size && term_ct < max; ) {
		uint64_t len;
		unsigned char c = bytes.data[p];
		unsigned char *string_start;

		switch( c ) {
			case 251 : len = 0; 
					   break;

					   // works only because endianess of x86 is same as mysql
					   // protocol (little endian)
			case 252 : 
					   // 2 bytes length
					   len = *(uint16_t * ) &bytes.data[p+1] ;
					   string_start = &bytes.data[p+3];
					   break;
			case 253 : 
					   
					   // 3 bytes length
					   len = *(uint16_t * ) &bytes.data[p+1] + ((uint32_t) bytes.data[p+3] << 16);
					   string_start = &bytes.data[p+4];
					   break;
			case 254 : 
					   // 8 bytes length
					   len = *(uint64_t * ) &bytes.data[p+1] ;
					   string_start = &bytes.data[p+9];
					   break;
			default :
					   // just one byte length
					   string_start= &bytes.data[p+1];
					   len=c;
				
		}

		if ( len > 0) {
			unsigned char *nextptr = &bytes.data[p+1+len];
			unsigned char cnext= *nextptr;
			*nextptr=0;  // terminate the current string with a NULL

			//printf("c=%02X p=%ld len=%ld s=%s\r\n", c, p, len, string_start);
			if ( len < 9 ) {
				int32_t v32 = strtol( (char*) string_start, NULL, 10);
				Terms[term_ct++]  = enif_make_int(env, v32);
			}
			else {
				int64_t v64 = strtoll( (char*) string_start, NULL, 10);
				Terms[term_ct++]  = enif_make_int(env, v64);
			}
			
			*nextptr=cnext;  // restore the byte 
			p += 1 + len;

		}
		else {
			p+=1;
		}
	}
	ERL_NIF_TERM ResultList = enif_make_list_from_array( env, Terms, term_ct);
	return ResultList;
}
int reload(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info) {
	return 0;
}

void unload ( ErlNifEnv* env, void* priv_data) {
}

int upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data, ERL_NIF_TERM load_info) {
	return 0;

}

static ErlNifFunc nif_funcs[] =
{
	    {"decode_row_data", 3, decode_row_data}
};

ERL_NIF_INIT(emysql_nif,nif_funcs,NULL,reload,upgrade,unload)
