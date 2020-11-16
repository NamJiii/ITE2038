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

typedef int bool;
#define true 1
#define false 0

#define PAGESIZE 4096

#define uframe buf_manager.frames[buf_num]
#define upage buf_manager.frames[buf_num].page
#define hpage buf_manager.frames[header_num].headerP
#define rpage buf_manager.frames[root_num].page
#define uupage buf_manager.frames[buf_num2].page
#define rcpage buf_manager.frames[right_num].page
#define lcpage buf_manager.frames[left_num].page
#define ppage buf_manager.frames[parent_num].page
#define cpage buf_manager.frames[child_num].page
#define npage buf_manager.frames[neighbor_num].page

#define order 32
#define internal_order 32




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

typedef struct buffer_structure{
	union{
		header_page headerP;
		page_t page;
	};
	int table_id;
	int page_num;
	bool is_dirty;
	bool is_pinned;
	int next_lru;
	int prev_lru;
}buffer_structure;

typedef struct buffer_manager{
	int capacity;
	int lru_index;
	buffer_structure * frames;
}buffer_manager;

buffer_manager buf_manager;

typedef struct qnode{
	off_t po;
	struct qnode* next;
}qnode;

typedef struct queue{
	int count;
	int pnum;
	struct queue* next;
}queue;

off_t num_to_off(pagenum_t pagenum){
	return PAGESIZE*pagenum;
}

pagenum_t off_to_num(off_t off){
	return off/PAGESIZE;
}


int init_db (int num_buf);
void file_read_page(int tid, pagenum_t pagenum, page_t* dest);
void file_write_page(int tid, pagenum_t pagenum, const page_t* src);

int close_table(int table_id);
int open_table (char *pathname);
int open_page(int tid, int pagenum);
int pick_up_page(int i);
int close_page(int buf_num);
int set_dirty(int buf_num);
void drop_page(int buf_num);
int shutdown_db(void);
void find_and_print(int table_id, int64_t key);
pagenum_t find_leaf(int table_id, int64_t key );
char * find(int table_id, int64_t key);
int cut( int length );

pagenum_t file_alloc_page(int table_id);
// INSERTION
record * make_record(int64_t key,char value[120]);
pagenum_t make_node( int table_id );
pagenum_t make_leaf( int table_id );
int get_left_index(int table_id, pagenum_t parent, pagenum_t left);
pagenum_t insert_into_leaf( int table_id, pagenum_t leaf, int64_t key, char * value );
pagenum_t insert_into_leaf_after_splitting(int table_id, pagenum_t leaf, int64_t key , char inserted_value[120]);
pagenum_t insert_into_node(int table_id, pagenum_t n, int left_index, int64_t key, pagenum_t right);
pagenum_t insert_into_node_after_splitting(int table_id, pagenum_t old_node, int left_index, int64_t key, pagenum_t right);
pagenum_t insert_into_parent(int table_id, pagenum_t left, int64_t key, pagenum_t right);
pagenum_t insert_into_new_root(int table_id, pagenum_t left, int64_t key, pagenum_t right);
pagenum_t start_new_tree(int table_id,int64_t key, char * value);
int insert(int table_id,int64_t key, char value[120] );


int get_neighbor_index( int table_id ,pagenum_t n );
pagenum_t remove_entry_from_node(int table_id, pagenum_t n, int64_t key, char value[120]);
pagenum_t adjust_root(int table_id);
pagenum_t coalesce_nodes(int table_id, pagenum_t n, pagenum_t neighbor, int neighbor_index, int k_prime);
pagenum_t redistribute_nodes(int table_id, pagenum_t n, pagenum_t neighbor, int neighbor_index, int k_prime_index, int k_prime);
pagenum_t delete_entry(int table_id, pagenum_t n, int64_t key, char value[120] );
pagenum_t delete(int table_id, int key);

