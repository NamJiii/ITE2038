#include<stdio.h>
#include<unistd.h>
#include<fcntl.h>
#include<errno.h>
#include<string.h>
#include<stdlib.h>
#include<inttypes.h>
#include<sys/types.h>

#define BUFFSIZE 4096

typedef int pagenum_t;

#define PAGESIZE 4096

typedef struct record {
	int64_t key;//64bit Á¤¼ö 
	char value[120];
} record;


typedef struct key_and_offset {
	int64_t key;
	off_t child_off;
} KeyOff;


typedef struct page_t { // 4096Byte
	// in-memory page struccture
	off_t parent;
	int is_leaf;
	int num_keys;
	char reserved[104];
	off_t ex_off;
	union {
		KeyOff branches[248]; // (internal) MAX 248 (key8+offset8) 
		record records[31]; // leaf MAX 31 (key8+offset120) 
	};
}page_t;

typedef struct header_page {
	off_t free_off;//[0-7] free page offset
	off_t root_off;//[8-15] root page offset
	int64_t num_pages;// [16-23] number of pages
	char reserved[4072];//reserved
} header_page;

typedef struct qnode{
	off_t po;
	struct qnode* next;
}qnode;

typedef struct queue{
	int count;
	int pnum;
	struct queue* next;
}queue;

int open_db(char *pathname);
off_t num_to_off(pagenum_t pagenum);
pagenum_t off_to_num(off_t off);
pagenum_t file_alloc_page();
void file_free_page(pagenum_t pagenum);
void file_read_page(pagenum_t pagenum, page_t* dest);
void file_write_page(pagenum_t pagenum, const page_t* src);
void find_and_print(int64_t key);
pagenum_t find_leaf(int64_t key);
char * find(int64_t key);

int cut( int length ) ;
record * make_record(int64_t key, char value[120]);
pagenum_t make_node( void );
pagenum_t make_leaf( void );
int get_left_index(pagenum_t parent, pagenum_t left) ;
pagenum_t insert_into_parent(pagenum_t left, int64_t key, pagenum_t right);
pagenum_t insert_into_new_root(pagenum_t left, int64_t key, pagenum_t right);

pagenum_t dequeue();
void enqueue(pagenum_t pagenum);
void init_queue(queue *q);
