#include<iostream>
#include<sstream>
#include<vector>
#include<assert.h>
#include<set>
#include<map>
#include<unordered_map>
#include<memory>

#include "bpt.h"

#define MAX 10

using namespace std;

int Tables[MAX];
int table_cnt;


vector<vector<page_t> > in_memory_pages;
vector< header_page > in_memory_header;
vector< vector< vector<int> > > num_same_element;


int init_db (int num_buf){
	buf_manager.capacity = num_buf;
	buf_manager.lru_index=-1;
	buf_manager.frames = (buffer_structure *)calloc(num_buf, sizeof(buffer_structure));
	int i;
	for(i=0; i<num_buf;i++){
		buf_manager.frames[i].next_lru=-1;
		buf_manager.frames[i].prev_lru=-1;	
	}
}

void file_read_page(int tid, pagenum_t pagenum, page_t* dest){
	int temp;
	temp = pread(tid, dest, PAGESIZE, num_to_off(pagenum)); 
}

void file_write_page(int tid, pagenum_t pagenum, const page_t* src){
	int temp;
	temp = pwrite(tid, src, PAGESIZE, num_to_off(pagenum));
}

int close_table(int table_id){

	//저장하는 기능 
	int i,j;
	int buf_num;
	int tmp,tmp2;
	
	for(buf_num=0; buf_num < buf_manager.capacity ; buf_num++){
		if(uframe.table_id == table_id){
			drop_page(buf_num);
			tmp = uframe.next_lru;
			if(buf_manager.lru_index==buf_num){
				buf_manager.lru_index= tmp;
				buf_manager.frames[tmp].prev_lru = -1;
			}
			else{
			tmp = uframe.next_lru;
			tmp2 = uframe.prev_lru;
			buf_manager.frames[tmp].prev_lru = tmp2;
			buf_manager.frames[tmp2].next_lru = tmp;
			}
		}
	}
	
	
	//table 배열에서 삭제 
	for(i=0;i<=table_cnt;i++){
		if(Tables[i]==table_id){
			for(j=i;j<table_cnt;j++){
				Tables[j]=Tables[j+1];
			}
			Tables[table_cnt]=-1;
			table_cnt--;
			return table_cnt;
		}
	}
	
	printf("table wasn't open\n");
	return -1;
}


int set_page_info(int tid, int pagenum, int buf_num){
	file_read_page(tid,pagenum,&upage);
	uframe.table_id =tid;
	uframe.page_num = pagenum;
	return tid;
}

int open_page(int tid, int pagenum){
	int i;
	int tmp;
	for(i=0; i<buf_manager.capacity ; i++){
		if(buf_manager.frames[i].table_id == tid &&buf_manager.frames[i].page_num == pagenum ){
			return pick_up_page(i);
		}
	}
	
	//lru index
	for(i=0; i<buf_manager.capacity; i++){
		if(buf_manager.frames[i].next_lru == -1 &&buf_manager.frames[i].prev_lru == -1 &&i!=buf_manager.lru_index){
			buf_manager.frames[buf_manager.lru_index].prev_lru = i;
			buf_manager.frames[i].next_lru = buf_manager.lru_index;
			buf_manager.lru_index=i;
			file_read_page(tid,pagenum,&buf_manager.frames[i].page);
			buf_manager.frames[i].table_id = tid;
			buf_manager.frames[i].page_num = pagenum;
			return pick_up_page(i);
		}		
	}
	
	tmp = buf_manager.lru_index;

	while(1){
		if(buf_manager.frames[tmp].is_pinned){
			tmp = buf_manager.frames[tmp].next_lru;
			if(tmp == -1){
				printf(" all pages are pinned now \n");
				return -1;
			}
		}	
		else break;
	}
	
	drop_page(tmp);

	file_read_page(tid,pagenum,&buf_manager.frames[tmp].page);
	buf_manager.frames[tmp].table_id = tid;
	buf_manager.frames[tmp].page_num = pagenum;
	buf_manager.frames[tmp].is_dirty = 0;
	return pick_up_page(tmp);
}


int close_page(int buf_num){
	uframe.is_pinned = 0;
	return  buf_num;
}

int set_dirty(int buf_num){
	uframe.is_dirty = 1;
	return buf_num;
}


int shutdown_db(void){
	while(table_cnt!=0){
		close_table(Tables[0]);
	}
	int i;

	return 0;
}
//FIND

void find_and_print(int table_id, int64_t key) {
    int64_t * r = find(table_id, key);
    if (r == NULL)
        printf("Record not found under key %lld.\n", key);
    else 
        printf("Record -- key %lld, first value %d.\n",
                 key, r[0]);
    free(r);
}//done do not need


int cut( int length ) {
    if (length % 2 == 0)
        return length/2;
    else
        return length/2 + 1;
}

// INSERTION
pagenum_t file_alloc_page(int table_id){
	int i;
	page_t* c;
	int header_num = open_page(table_id,0);
	int buf_num;
	
	int tmp = hpage.num_pages;
	for(i=9; i>=0; i--){
		buf_num = open_page(table_id,tmp+i);
		upage.parent=hpage.free_off;//이때 parent 는 그냥 next free page
		set_dirty(buf_num); 
		hpage.free_off = num_to_off(tmp+i);
		set_dirty(header_num);
		close_page(buf_num);
	}
	hpage.num_pages = tmp+10;
	set_dirty(header_num);
	close_page(header_num);
	return tmp;
}// buffer D

// INSERTION
record * make_record(int64_t key,int64_t* values) {
	if(sizeof(values)>120){
		printf("Value must be shorter than 120 bytes");
		return 0;
	}
    record * new_record = (record *)calloc(1,sizeof(record));
    if (new_record == NULL) {
        perror("Record creation.");
        exit(EXIT_FAILURE);
    }
    else {
    	memcpy(&new_record->values,values,sizeof(values));
        new_record->key = key;
    }
    return new_record;
}

pagenum_t make_node( int table_id ) {
    
    pagenum_t pagenum;
    int header_num = open_page(table_id,0);
    if(hpage.free_off==0) pagenum = file_alloc_page(table_id);
	
	else{
		off_t temp = hpage.free_off;
		pagenum = off_to_num(temp);
	}
	
	int buf_num = open_page(table_id,pagenum);
	
//	page_t* tempP =  (page_t *) calloc(1,PAGESIZE);
//	file_read_page(pagenum,tempP);

	if (buf_num == -1) {
		close_page(buf_num);
   	 	perror("Node creation.");
    	exit(EXIT_FAILURE);
    }
				
	hpage.free_off = upage.parent;
	set_dirty(header_num);
	upage.parent = 0;
	set_dirty(buf_num);
	
	close_page(header_num);
	close_page(buf_num);

	return pagenum;
}//buffer D

pagenum_t make_leaf( int table_id ) {
    pagenum_t pagenum = make_node(table_id);
    int buf_num = open_page(table_id,pagenum);
    upage.is_leaf = true;
    set_dirty(buf_num);
    close_page(buf_num);
    return pagenum;
}//buffer D

int get_left_index(int table_id, pagenum_t parent, pagenum_t left) {

    int left_index = -1;
    
    int parent_num = open_page(table_id,parent);
    int left_num = open_page(table_id,left);
    
    while (left_index <= ppage.num_keys && 
            ppage.branches[left_index].child_off != num_to_off(left))
        left_index++;
    
    close_page(left_num);
    close_page(parent_num);

    return left_index;
}

pagenum_t insert_into_leaf( int table_id, pagenum_t leaf, int64_t key, int64_t * values ) {

	int buf_num = open_page(table_id,leaf);
	
    int i, insertion_point;

    insertion_point = 0;
    while (insertion_point < upage.num_keys && upage.records[insertion_point].key < key)
        insertion_point++;

    for (i = upage.num_keys; i > insertion_point; i--) {
    	memcpy(upage.records[i].values,upage.records[i-1].values,sizeof(upage.records[i-1].values));
    	upage.records[i].key = upage.records[i-1].key;
    }
    
    memcpy(upage.records[insertion_point].values,values,sizeof(values));
    upage.records[insertion_point].key = key;
    upage.num_keys++;
    
    set_dirty(buf_num);
    close_page(buf_num);
    
    return leaf;
}//buffer D

pagenum_t insert_into_leaf_after_splitting(int table_id, pagenum_t leaf, int64_t key , int64_t* inserted_values) {

	int buf_num = open_page(table_id, leaf);
	
	pagenum_t new_pagenum = make_leaf(table_id);
	
	int buf_num2 = open_page(table_id,new_pagenum);
	
    int insertion_index, split, i, j;
    int64_t new_key;

	int64_t temp_keys[order];
	int64_t* temp_values[order];

    insertion_index = 0;
    while (insertion_index < order - 1 && upage.records[insertion_index].key < key)
        insertion_index++;

    for (i = 0, j = 0; i < upage.num_keys; i++, j++) {
        if (j == insertion_index) j++;
        temp_values[j] = upage.records[i].values;
        temp_keys[j] = upage.records[i].key;
    }

    temp_values[insertion_index] = inserted_values;
    temp_keys[insertion_index] = key;

    upage.num_keys = 0;

    split = cut(order - 1);

    for (i = 0; i < split; i++) {
    	memcpy(upage.records[i].values,&temp_values[i],sizeof(temp_values[i]));
	    upage.records[i].key = temp_keys[i];
        upage.num_keys++;
    }

    for (i = split, j = 0; i < order; i++, j++) {
    	memcpy(uupage.records[j].values,&temp_values[i],sizeof(temp_values[i]));
     	uupage.records[j].key = temp_keys[i];
        uupage.num_keys++;
    }

    uupage.ex_off = upage.ex_off;
    upage.ex_off = num_to_off(new_pagenum);

    for (i = upage.num_keys; i < order - 1; i++){
        upage.records[i].key = 0;}

    for (i = upage.num_keys; i < order - 1; i++){
        uupage.records[i].key = 0;
	}
		 
    uupage.parent = upage.parent;
    new_key = uupage.records[0].key;
    
    set_dirty(buf_num);
    set_dirty(buf_num2);
    close_page(buf_num);
    close_page(buf_num2);

    return insert_into_parent(table_id,leaf, new_key, new_pagenum);
}//buffer D

pagenum_t insert_into_node(int table_id, pagenum_t n, int left_index, int64_t key, pagenum_t right) {
    int i, temp;
	int header_num = open_page(table_id,0);
	int buf_num = open_page(table_id,n);
	int right_num = open_page(table_id,right);
	
    for (i = upage.num_keys; i > left_index; i--) {
    	upage.branches[i+1].key = upage.branches[i].key;
        upage.branches[i+1].child_off = upage.branches[i].child_off; //
    }
    upage.branches[left_index + 1].child_off = num_to_off(right);
    upage.branches[left_index+1].key = key;
    upage.num_keys++;
    
    set_dirty(buf_num);
	close_page(right_num);
	close_page(buf_num);
	close_page(header_num);  

    return n;
}

pagenum_t insert_into_node_after_splitting(int table_id, pagenum_t old_node, int left_index, 
												int64_t key, pagenum_t right) {
													
	int header_num = open_page(table_id,0);
	
    int i, j, split, k_prime;
    
    int root_num = open_page(table_id,off_to_num(hpage.root_off));
    int buf_num = open_page(table_id, old_node); // oldnode
    int right_num = open_page(table_id,right);// right
    
	int64_t temp_keys[internal_order];
	off_t temp_off[internal_order];

    for (i = 0, j = 0; i < upage.num_keys; i++, j++) {
		if (j == left_index + 1) j++;
		temp_off[j] = upage.branches[i].child_off;
	}

	for (i = 0, j = 0; i < upage.num_keys; i++, j++) {
		if (j == left_index + 1) j++;
		temp_keys[j] = upage.branches[i].key;
	}

	temp_keys[left_index + 1] = key;
	temp_off[left_index + 1] = num_to_off(right);
 
    split = cut(internal_order-1);
    int new_node = make_node(table_id);
    int buf_num2 = open_page(table_id,new_node);
    
    upage.num_keys = 0;
    
    for (i = 0; i < split; i++) {
        upage.branches[i].key = temp_keys[i];
        upage.branches[i].child_off = temp_off[i];
        upage.num_keys++;
    }
        
    uupage.ex_off = temp_off[split];
	k_prime = temp_keys[split];
	uupage.num_keys = 0;

	for (i = split + 1, j = 0; i < internal_order; i++, j++) {
		uupage.branches[j].child_off = temp_off[i];
		uupage.branches[j].key = temp_keys[i];
		uupage.num_keys++;
	}
    
    uupage.parent = upage.parent;
    int child_num;
    for (i = 0; i <= uupage.num_keys; i++) {
    	child_num = open_page(table_id,off_to_num(uupage.branches[i].child_off));
    	cpage.parent = num_to_off(new_node);
    	set_dirty(child_num);
    	close_page(child_num);
    }
    
    set_dirty(buf_num2);
    set_dirty(buf_num);
    set_dirty(right_num);
    
    close_page(buf_num2);
    close_page(buf_num);
    close_page(right_num);
    close_page(root_num);
    close_page(header_num);
	    
    return insert_into_parent(table_id, old_node, k_prime, new_node);
}

pagenum_t insert_into_parent(int table_id, pagenum_t left, int64_t key, pagenum_t right) {
    int left_index;
	int left_num = open_page(table_id,left);
	int right_num = open_page(table_id,right);
	int parent_num = open_page(table_id,off_to_num(lcpage.parent));
	
    /* Case: new root. */

    if (lcpage.parent == 0){
    	close_page(left_num);
    	close_page(right_num);
    	close_page(parent_num);
        return insert_into_new_root(table_id, left, key, right);
    }
    
    left_index = get_left_index(table_id, off_to_num(lcpage.parent), left);

     int64_t tmp =ppage.num_keys;
     off_t parentoff = off_to_num(lcpage.parent);
     
    close_page(left_num);
    close_page(right_num);
    close_page(parent_num);

    if (tmp < internal_order - 1)
        return insert_into_node(table_id, parentoff, left_index, key, right);

    return insert_into_node_after_splitting(table_id, parentoff, left_index, key, right);
}

pagenum_t insert_into_new_root(int table_id, pagenum_t left, int64_t key, pagenum_t right) {

	int left_num = open_page(table_id, left);
	int right_num = open_page(table_id,right);
	int header_num = open_page(table_id,0);

    pagenum_t root = make_node(table_id);
    
    int root_num = open_page(table_id,root);

    rpage.ex_off= num_to_off(left);
    rpage.branches[0].key = key; // minimum value;
    rpage.branches[0].child_off = num_to_off(right);
    rpage.num_keys++;
    rpage.parent = 0;

    lcpage.parent = num_to_off(root);
    rcpage.parent = num_to_off(root);
    hpage.root_off = num_to_off(root);
    
    set_dirty(header_num);
    set_dirty(root_num);
    set_dirty(right_num);
    set_dirty(left_num);
    
    close_page(header_num);
    close_page(root_num);
    close_page(right_num);
    close_page(left_num);
    
    return root;
}

pagenum_t start_new_tree(int table_id,int64_t key, int64_t * values) {
	
    pagenum_t root = make_leaf(table_id);
	int root_num = open_page(table_id,root);
	int header_num = open_page(table_id,0);
	
	hpage.root_off = num_to_off(root);
	set_dirty(header_num);
	
	memcpy(rpage.records[0].values, values,sizeof(values));	
	rpage.records[0].key = key;
	rpage.parent=0;
	rpage.num_keys++;
	set_dirty(root_num);
	
	close_page(header_num);
	close_page(root_num);

    return root;
}

int insert(int table_id,int64_t key, int64_t* values ) {

    pagenum_t leaf;
    
    int buf_num;
	
	int header_num;
	
	if (find(table_id,key) != NULL){
    	return 0;
    }
		
	header_num = open_page(table_id,0);

	
	int root = off_to_num(hpage.root_off);
	close_page(header_num);

    if (root == 0)
        return start_new_tree(table_id,key, values);
        
	int root_num = open_page(table_id,root);


    leaf = find_leaf(table_id, key);
    buf_num = open_page(table_id,leaf);

    if (upage.num_keys < order - 1) {
    	close_page(buf_num);
        leaf = insert_into_leaf(table_id,leaf, key, values);
        
        return leaf;
    }

     close_page(buf_num);

    return insert_into_leaf_after_splitting(table_id,leaf, key, values);
}// buffer D

// DELETION.

int get_neighbor_index( int table_id ,pagenum_t n ) {

    int i;
	int buf_num = open_page(table_id, n);
	int parent_num = open_page(table_id, off_to_num(upage.parent));

    for (i = -1; i <= ppage.num_keys; i++)
        if (ppage.branches[i].child_off == num_to_off(n)){
        	close_page(buf_num);
        	close_page(parent_num);
        	return i - 1;
		}
            
    // Error state.
    printf("Search for nonexistent pointer to node in parent.\n");
    printf("Node:  %#lx\n", (unsigned long)n);
    exit(EXIT_FAILURE);
}


pagenum_t remove_entry_from_node(int table_id, pagenum_t n, int64_t key/*, char value[120]*/) {

    int i, num_pointers;
	
	int buf_num = open_page(table_id,n);
	
    // Remove the key and shift other keys accordingly.
    i = 0;
    if(upage.is_leaf){
	    while (upage.records[i].key != key)
	        i++;
	    for (++i; i < upage.num_keys; i++){
	        upage.records[i - 1].key = upage.records[i].key;
	        memcpy(upage.records[i - 1].values, upage.records[i].values, sizeof(upage.records[i].values));
			}
    }
    
    else{
	    while (upage.branches[i].key != key)
	        i++;
	    for (++i; i < upage.num_keys; i++){
	        upage.branches[i - 1].key = upage.branches[i].key;
	        upage.branches[i - 1].child_off = upage.branches[i].child_off; } 	
    }

    upage.num_keys--;

    if (upage.is_leaf)
        for (i = upage.num_keys; i < internal_order - 1; i++)
            upage.records[i].values[0]='\0';
    else
        for (i = upage.num_keys + 1; i < internal_order; i++)
            upage.branches[i].child_off = 0; 
	set_dirty(buf_num);
	close_page(buf_num);
    return n;
}


pagenum_t adjust_root(int table_id) {
	int header_num = open_page(table_id,0);
	int root_num = open_page(table_id,off_to_num(hpage.root_off));

    if (rpage.num_keys > 0){
		int temp = off_to_num(hpage.root_off);
    	close_page(root_num);
    	close_page(header_num);
        return temp;
	}
	
    off_t new_root_off;
	
    if (!rpage.is_leaf) {
    	new_root_off = rpage.branches[-1].child_off;
    	int buf_num2 = open_page(table_id,off_to_num(new_root_off));
        uupage.parent = 0;
        set_dirty(buf_num2);
        close_page(buf_num2);
    }


    else new_root_off = 0;
    hpage.root_off = new_root_off;
    set_dirty(header_num);
    close_page(header_num);
    close_page(root_num);

    return new_root_off;
}



pagenum_t coalesce_nodes(int table_id, pagenum_t n, pagenum_t neighbor, int neighbor_index, int k_prime) {
    int i, j, neighbor_insertion_index, n_end;
    pagenum_t root;
    int buf_num = open_page(table_id, n);
    int neighbor_num = open_page(table_id,neighbor);

	pagenum_t temp;

//	page_t* tempP = calloc(1,PAGESIZE);

    if (neighbor_index == -2) {
    	temp = buf_manager.frames[buf_num].page_num;
    	buf_manager.frames[buf_num].page_num = buf_manager.frames[neighbor_num].page_num;
    	buf_manager.frames[neighbor_num].page_num = temp;
    	
    	set_dirty(buf_num);
    	set_dirty(neighbor_num);
    }

    neighbor_insertion_index = npage.num_keys;
    
    if (!upage.is_leaf) {

        npage.branches[neighbor_insertion_index/*-1*/].key = k_prime; 
        npage.num_keys++;


        n_end = upage.num_keys;

        for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++) {
            npage.branches[i].key = upage.branches[j].key;
            npage.branches[i].child_off = upage.branches[j].child_off;
            npage.num_keys++;
            upage.num_keys--;
        }

        npage.branches[i].child_off = upage.branches[j].child_off;


        for (i = 0; i < npage.num_keys + 1; i++) {
            temp = npage.branches[i].child_off;
            int buf_num2 = open_page(table_id, temp);
            uupage.parent = num_to_off(neighbor);
            set_dirty(buf_num2);
            close_page(buf_num2);
        }
    }


    else {
        for (i = neighbor_insertion_index, j = 0; j < upage.num_keys; i++, j++) {
            npage.records[i].key = upage.records[j].key;
            memcpy(npage.records[i].values, upage.records[j].values,sizeof(upage.records[j].values));
			npage.num_keys++;
        }
        memcpy(npage.records[order-1].values, upage.records[order-1].values,sizeof(upage.records[order-1].values));
    }
    set_dirty(buf_num);
    set_dirty(neighbor_num);
	
    root = erase_entry(table_id, off_to_num(upage.parent), k_prime/*, upage.records[0].value*/);
    
    close_page(buf_num);
    close_page(neighbor_num);
    return root;
}


pagenum_t redistribute_nodes(int table_id, pagenum_t n, pagenum_t neighbor, int neighbor_index, 
        int k_prime_index, int k_prime) {  

	int buf_num = open_page(table_id, n);
	int neighbor_num = open_page(table_id,neighbor);
	int parent_num;
	int buf_num2;
	
    int i;
    int tmp;
    
//    page_t * tmpP = calloc(1,PAGESIZE);
//	page_t * parentP = calloc(1,PAGESIZE);

    if (neighbor_index != -2) {


        if (!upage.is_leaf) {
        	for (i = upage.num_keys; i > 0; i--) {
            upage.branches[i].key = upage.branches[i - 1].key;
            upage.branches[i].child_off = upage.branches[i - 1].child_off;
       		 }
			upage.branches[0].child_off = upage.ex_off;
            upage.branches[-1].child_off = upage.branches[npage.num_keys].child_off;
            tmp = upage.branches[-1].child_off;
            buf_num2 = open_page(table_id,tmp);
            uupage.parent = num_to_off(n);
            npage.branches[npage.num_keys].child_off = 0;
            upage.branches[0].key = k_prime;
            parent_num = open_page(table_id,off_to_num(upage.parent));
            ppage.branches[k_prime_index].key = npage.records[npage.num_keys].key;
        }
        else {
        	for (i = upage.num_keys; i > 0; i--) {
            upage.records[i].key = upage.records[i - 1].key;
            memcpy(upage.records[i].values ,upage.records[i - 1].values,sizeof(upage.records[i - 1].values));
			}
        	memcpy(upage.records[0].values, npage.records[npage.num_keys - 1].values,sizeof(npage.records[npage.num_keys - 1].values));
			npage.records[npage.num_keys-1].values[0]='\0';// 이건좀.... 11.25 
            upage.records[0].key = npage.records[npage.num_keys - 1].key;
            parent_num = open_page(table_id,off_to_num(upage.parent));
            ppage.records[k_prime_index].key = upage.records[0].key;
        }
    }

    else {  
        if (upage.is_leaf) {
            upage.records[upage.num_keys].key = npage.records[0].key;
            memcpy(upage.records[upage.num_keys].values, npage.records[0].values,sizeof(npage.records[0].values));
			parent_num = open_page(table_id,off_to_num(upage.parent));
            ppage.records[k_prime_index].key = npage.records[1].key;
        }
        else {
            upage.branches[upage.num_keys].key = k_prime;
            upage.branches[upage.num_keys + 1].child_off = npage.branches[0].child_off;
            tmp = upage.branches[upage.num_keys + 1].child_off;
/*여기부터 보수공사*/ 
			buf_num2 = open_page(table_id,off_to_num(tmp));
			parent_num = open_page(table_id,off_to_num(uupage.parent));
			int temp = off_to_num(ppage.parent);
			close_page(parent_num);
			parent_num = open_page(table_id,temp); // 좀 이상한데...? 
            ppage.branches[k_prime_index].key = npage.branches[0].key;
        }
        for (i = 0; i < npage.num_keys - 1; i++) { 
            npage.records[i].key = npage.records[i + 1].key;
            memcpy(npage.records[i].values, npage.records[i + 1].values,sizeof(npage.records[i + 1].values));
        }
        if (!upage.is_leaf)
			npage.branches[i].child_off = npage.branches[i + 1].child_off;
    }

    upage.num_keys++;
    npage.num_keys--;

	set_dirty(buf_num);
	set_dirty(neighbor_num);
	set_dirty(parent_num);
	
	close_page(buf_num);
	close_page(neighbor_num);
	close_page(buf_num2);
	close_page(parent_num);
	
    return 0;
}// 이거 틀릴가능성 매우높 

pagenum_t erase_entry(int table_id, pagenum_t n, int64_t key/*, char value[120]*/) {

    int min_keys;
    pagenum_t neighbor;
	int header_num = open_page(table_id,0);
	int root_num = open_page(table_id,off_to_num(hpage.root_off));
	
    page_t * neighborP = (page_t *)calloc(1,PAGESIZE); 
    int neighbor_index;
    int k_prime_index, k_prime;
    int capacity;
	page_t* parentP = (page_t *)calloc(1,PAGESIZE);

	page_t * nP = (page_t *)calloc(1,PAGESIZE);
	
    n = remove_entry_from_node(table_id,n, key/*, value*/);

    if (n == off_to_num(hpage.root_off)) 
        return adjust_root(table_id);

	int buf_num = open_page(table_id, n);

    min_keys = upage.is_leaf ? (cut(order - 1))/2 : (cut(internal_order) - 1)/2;


    if (upage.num_keys >= min_keys)
        return off_to_num(hpage.root_off);

    neighbor_index = get_neighbor_index(table_id, n );
    k_prime_index = neighbor_index == -2 ? 0 : neighbor_index+1;
    int parent_num = open_page(table_id,off_to_num(upage.parent));

    k_prime = ppage.branches[k_prime_index].key;

    neighbor = neighbor_index == -2 ? off_to_num(ppage.branches[0].child_off) : 
        off_to_num(ppage.branches[neighbor_index].child_off);

    capacity = upage.is_leaf ? order : internal_order - 1;
	
	int neighbor_num = open_page(table_id,neighbor);
    
    // Coalescence.
    if (npage.num_keys + upage.num_keys < capacity)
        return coalesce_nodes(table_id, n, neighbor, neighbor_index, k_prime);

    //Redistribution.

    else
        return redistribute_nodes(table_id, n, neighbor, neighbor_index, k_prime_index, k_prime);
}



//Master deletion function.

pagenum_t erase(int table_id, int key) {

    int64_t key_leaf;
    int64_t * key_value = find(table_id,key);


    key_leaf = find_leaf(table_id,key);
    if (key_value != NULL && key_leaf != NULL) {
        key_leaf = erase_entry(table_id, key_leaf, key/*, key_value*/);
    }
    return key_leaf;
}

typedef struct tableinfo {
    header_page headerP;
    int tid;
} t_info;

vector<t_info> tables;

int num_element(t_info* table){
	int i;
	int result=0;
	for(i=0; i<table->headerP.num_pages-1 ; i++){
		if(in_memory_pages[table->tid-1][i].is_leaf){
		result += in_memory_pages[table->tid-1][i].num_keys;}
	}
	return result;
}

void tbl_key(t_info* table, int64_t keys[]){
	int i, j;
	int keyN;
	int keyit=0;
	for(i=0; i<table->headerP.num_pages-1; i++){
		if(in_memory_pages[table->tid-1][i].is_leaf){
		keyN = in_memory_pages[table->tid-1][i].num_keys;
		for(j=0; j<keyN; j++){
			keys[keyit++] = in_memory_pages[table->tid-1][i].records[j].key;
		}}
	}	
}

struct cInfo{
	t_info *table;
	int col;
	bool operator == (const cInfo &o) const{
		return table->tid == o.table->tid && col == o.col;
	}
	bool operator <(const cInfo &o) const{
		return table->tid < o.table->tid || (table->tid == o.table->tid && col < o.col);
	}
};


struct Oprns{
	cInfo left;
	cInfo right;
}; 

class base{
	public:
		vector<vector<int64_t>> result;
		map<cInfo, int> uCol;
		int cntcol = 0;

		virtual bool select(const cInfo &s){return false;}
		virtual void run() {};
};

class needs : public base{
	public:
		t_info *table;

		needs(t_info *table) : table(table){}
		
		virtual bool select(const cInfo &s){
			if(table->tid != s.table->tid) return false;
			if(!uCol.count(s)){
				uCol[s] = cntcol++;
			}
			return true; 
		}
		
		virtual void run(){
			vector<int64_t> keys;
			keys.resize(num_element(table));
			tbl_key(table, &keys[0]);
			for(int i = 0; i < num_element(table) ; i++){
				vector<int64_t> row;
				row.resize(uCol.size());
				int64_t *values = find(table->tid,keys[i]);
				for(auto &it : uCol){
					row[it.second] = values[it.first.col -2];
					if(it.first.col == 1){row[it.second] = keys[i];}
				}
				result.emplace_back(move(row));
			}
		}
}; 

 class Join : public base{
 	public:
 		unique_ptr<base> left;
 		unique_ptr<base> right;
 		map<cInfo, int> left_uCol;
 		map<cInfo, int> right_uCol;
 		
 		Oprns p;

 		Join(unique_ptr<base> &&left, unique_ptr<base> &&right, Oprns &p):left(move(left)), right(move(right)), p(p){}
 		
 		virtual bool select(const cInfo &s){
 			if(left->select(s)){
 				if(!left_uCol.count(s)){
 					uCol[s] = cntcol;
 					left_uCol[s] = cntcol++;
 				}
 				return true;
 			}
 			else if(right->select(s)){
 				if(!right_uCol.count(s)){
 					uCol[s] = cntcol;
 					right_uCol[s] = cntcol++;
				 }
				 return true;
			 }
			 return false;
		}
		
		virtual void run(){
			preset();
			auto &left_result = left->result;
			auto &right_result = right->result;
			unordered_multimap<int64_t, vector<int64_t>>hash_table;
			int left_key_column = left->uCol[p.left];
			for( auto &left_row : left_result){
				hash_table.emplace(left_row[left_key_column],left_row);
			}
			int right_key_column = right->uCol[p.right];
			for(auto &right_row : right_result){
				auto range = hash_table.equal_range(right_row[right_key_column]);
				for(auto it = range.first; it!= range.second; ++it){
					auto &left_row = it->second;
					vector<int64_t> joined_row;
					joined_row.resize(left_uCol.size()+right_uCol.size());
					for(auto &it : left_uCol){
						joined_row[it.second] = left_row[left->uCol[it.first]];
					}
					for(auto &it : right_uCol){
						joined_row[it.second] = right_row[right->uCol[it.first]];
					}
					result.emplace_back(joined_row);
				}
			}
		}
		
		void preset(){
			if(!left->select(p.left)){
				swap(p.left, p.right);
			}
			left->select(p.left);
			right->select(p.right);
			left->run();
			right->run();
			const auto &left_result = left->result;
			const auto &right_result = right->result;			
		}
};
	
class SelfJoin : public base{
	public:
		unique_ptr<base> input;
		Oprns p;

		SelfJoin(unique_ptr<base> &&input, Oprns &p) : input(move(input)),p(p){}
		
		virtual bool select(const cInfo &s){
			bool res = input->select(s);
			if(res && !uCol.count(s)){
				uCol[s] = cntcol++;
			}
			return true;
		}
		
		virtual void run(){
			preset();
			const auto &input_result = input->result;
			int left_key_column = input->uCol[p.left];
			int right_key_column = input->uCol[p.right];
			for(auto &row : input_result){
				if(row[left_key_column] == row[right_key_column] ){
					vector<int64_t> joined_row;
					joined_row.resize(uCol.size());
					for(auto &it : uCol){
						joined_row[it.second] = row[input->uCol[it.first]];
					}
					result.emplace_back(joined_row);
				}
			} 
		}
		
		void preset(){
			input->select(p.left);
			input->select(p.right);
			input->run();			
		}
};

int64_t summation(unique_ptr<base> &&input, vector<cInfo> &&keys){
	for(auto &s : keys){
 		input ->select(s);
	}
	input->run();
	const auto &result = input->result;
	int64_t total = 0;
	
	for(auto &row : result){
		for(auto &val : row){
			total +=val;
		}
	}
	return total;
}

vector<Oprns> parse(string query){
	vector<Oprns> oprns; 
	istringstream iss(query);
	
	int a,b,c,d;
	char t;
	Oprns oprn;
	
	iss >> a >> t >> b >> t >> c >> t >> d;

	oprn.left.table = &tables[a-1];
	oprn.left.col = b;
	oprn.right.table = &tables[c-1];
	oprn.right.col = d;
	oprns.push_back(oprn);
	
	while(iss>>t && t== '&'){
		iss >> a >> t >> b >> t >> c >> t >> d;

	oprn.left.table = &tables[a-1];
	oprn.left.col = b;
	oprn.right.table = &tables[c-1];
	oprn.right.col = d;
	oprns.push_back(oprn);

	}
	return oprns;
} 

int64_t join(const char* query){
	vector<Oprns> ops = parse(query);
	vector<cInfo> keys;
	set<int> used_tables;
	auto left = make_unique<needs>(ops[0].left.table);
	auto right = make_unique<needs>(ops[0].right.table);
	unique_ptr<base> root = make_unique<Join>(move(left), move(right), ops[0]);
	used_tables.insert(ops[0].left.table->tid);
	used_tables.insert(ops[0].right.table->tid);
	keys.push_back({ops[0].left.table,1});
	keys.push_back({ops[0].right.table,1});
	for(int i = 1 ; i < ops.size() ; i++){
		bool left_exist = used_tables.count(ops[i].left.table->tid);
		bool right_exist = used_tables.count(ops[i].right.table->tid);
		if(left_exist && right_exist){
			unique_ptr<base> new_root = make_unique<SelfJoin>(move(root), ops[i]);
			root = move(new_root);
		}
		else if (!left_exist && right_exist){
			auto left = make_unique<needs>(ops[i].left.table);
			unique_ptr<base> new_root = make_unique<Join>(move(left),move(root),ops[i]);
			used_tables.insert(ops[i].left.table->tid);
			root = move(new_root);
		}
		else if(left_exist && !right_exist){
			auto right = make_unique<needs>(ops[i].right.table);
			unique_ptr<base> new_root = make_unique<Join>(move(root),move(right),ops[i]);
			used_tables.insert(ops[i].right.table->tid);
			root = move(new_root);			
		}
		else{ 
			ops.push_back(ops[i]);
			continue;
		}
		keys.push_back({ops[i].left.table, 1});
		keys.push_back({ops[i].left.table, 1});
	}

	return summation(move(root),move(keys)); 
}

int open_table (char *pathname,int cntcol){
	
   Tables[table_cnt] = open(pathname, O_RDWR | O_SYNC);
   
   t_info temp_table;
    header_page* htemp;
   file_read_page(Tables[table_cnt],0,(page_t *)(&temp_table.headerP));
 
   temp_table.tid=table_cnt+1;
   tables.push_back(temp_table);

   	int i;
	page_t tempP;
	if(temp_table.headerP.num_pages!=0){
		vector<page_t> tmp_page_vec;
		for(i=1; i<temp_table.headerP.num_pages ; i++){
			file_read_page(Tables[table_cnt],i,&tempP);
			tmp_page_vec.push_back(tempP);
		}
		in_memory_pages.push_back(tmp_page_vec);
		tmp_page_vec.clear();
	}

   table_cnt++;
   return 0;
}

int pick_up_page(int i){
	int tmp, tmp2;
	tmp = buf_manager.frames[i].next_lru ;
	if(tmp==-1){
		buf_manager.frames[i].is_pinned = 1;
		tmp = i;
		while(buf_manager.frames[tmp].prev_lru!=-1){
		tmp = buf_manager.frames[tmp].prev_lru;
		}
		buf_manager.lru_index = tmp;
	
		return i;
	}
 
	tmp2 = buf_manager.frames[tmp].prev_lru ; 
	buf_manager.frames[tmp].prev_lru = buf_manager.frames[i].prev_lru;
	if(buf_manager.frames[i].prev_lru==-1) buf_manager.lru_index=i;
	else buf_manager.frames[buf_manager.frames[i].prev_lru].next_lru = tmp;
	
	while(buf_manager.frames[tmp].next_lru !=-1){
		tmp = buf_manager.frames[tmp].next_lru;
	}
	buf_manager.frames[tmp].next_lru = tmp2;
	buf_manager.frames[tmp2].prev_lru = tmp;
	buf_manager.frames[tmp2].next_lru = -1;
	buf_manager.frames[tmp2].is_pinned = 1;
	
	while(buf_manager.frames[tmp].prev_lru!=-1){
		tmp = buf_manager.frames[tmp].prev_lru;
	}
	buf_manager.lru_index = tmp;
	
	return tmp2;
}

void drop_page(int buf_num){
	uframe.table_id = 0;
	uframe.page_num=0;
}


pagenum_t find_leaf(int tid, int64_t key ) {
    int i = 0;
    int temp1, temp2;
    int buf_num;

	int pagenum = off_to_num(tables[tid-1].headerP.root_off);
	
	if(pagenum==0){
		return 0;
	}

	temp2 = pagenum; 
    while (!in_memory_pages[tid-1][pagenum-1].is_leaf) {
        i = -1;
        while (i <= in_memory_pages[tid-1][pagenum-1].num_keys) {
            if (key >= in_memory_pages[tid-1][pagenum-1].branches[i+1].key && i<in_memory_pages[tid-1][pagenum-1].num_keys-1) {
				i++;
            }
            else break;
        }
        if (i == in_memory_pages[tid-1][pagenum-1].num_keys){
        	return 0;	
        }
        else if(i==-1)
        	pagenum = off_to_num(in_memory_pages[tid-1][pagenum-1].ex_off);
        else
        	pagenum = off_to_num(in_memory_pages[tid-1][pagenum-1].branches[i].child_off);
    }
	
    return pagenum;
}


int64_t * find(int tid, int64_t key) {

    int i = 0;
    int buf_num;
    pagenum_t pagenum;

    pagenum = find_leaf( tid, key );

    if (pagenum == 0) return NULL;
    	
    	
    for (i = 0; i < in_memory_pages[tid-1][pagenum-1].num_keys; i++)
        if (in_memory_pages[tid-1][pagenum-1].records[i].key == key) break;
    
    if (i == in_memory_pages[tid-1][pagenum-1].num_keys) {
    	return NULL;
    }
    else{
    	return in_memory_pages[tid-1][pagenum-1].records[i].values;
    }
}

void print_table(int tid){
	t_info* printed = &tables[tid-1];
	printf("%d\n",printed->tid);
	printf("%d\n",printed->headerP.free_off);
	printf("%d\n",printed->headerP.root_off);//num_off
	printf("%d\n",printed->headerP.num_pages);//root_off
	printf("%d\n",printed->headerP.num_column);//num_page
	
	int num = num_element(printed);
	printf("%d\n",num);
	int64_t* value;
	tbl_key(printed, value);
	for(int i=0; i< num; i++){
		printf("%d ",value[i]);
	}
	printf("\n");
}

void print_keyvalue(int tid, int64_t key){
	t_info* table = &tables[tid-1];
	int64_t* value;
	value = find(table->tid,key);
	for(int i=0; i< table->headerP.num_column-1;i++){
		printf("%lld ",value[i]);
	}
	printf("\n");
}
/*
int main(){
	open_table("small-01.tab",0);
	open_table("small-02.tab",0);
	open_table("small-03.tab",0);
	open_table("small-04.tab",0);

	const char* query1 = "4.3=2.2&1.3=4.1";
	
//	while(true){
//		printf("input query\n");
//		scanf("%s",query);
//		const char* const_query = query;
//		if(query=="q") break;
//		else printf("%lld\n",parse_and_join(query));
//	}
printf("%lld",join(query1));

return 0;
}
*/