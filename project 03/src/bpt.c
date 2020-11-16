#include "buffer.h"

#define MAX 10

int Tables[MAX];
int table_cnt;

int init_db (int num_buf){
	buf_manager.capacity = num_buf;
	buf_manager.lru_index=-1;
	buf_manager.frames = calloc(num_buf, sizeof(buffer_structure));
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

int open_table (char *pathname){
   int temp;
   Tables[table_cnt] = open(pathname, O_RDWR | O_CREAT | O_EXCL /*| O_SYNC*/, 0777);

	int header_num = open_page(Tables[table_cnt],0);

   if (Tables[table_cnt] > 0) {
      hpage.num_pages = 1;
		table_cnt++;
		set_dirty(header_num);
		close_page(header_num);
      return Tables[table_cnt-1];
   }
   
	
   Tables[table_cnt] = open(pathname, O_RDWR /*| O_SYNC*/);
   if (Tables[table_cnt] > 0) {
      if (hpage.root_off != 0) {
      	int root_num = open_page(Tables[table_cnt],off_to_num(hpage.root_off));

         if (Tables[table_cnt] < PAGESIZE) {
            printf("%"PRId64"\n", hpage.root_off);
            printf("Failed to read root_page\n");
            printf("%s\n", strerror(errno));
            exit(EXIT_FAILURE);
         }
         
        close_page(root_num);
      }
      table_cnt++;
      
      set_dirty(header_num);
      close_page(header_num);


      return Tables[table_cnt-1];
   } 
	
    set_dirty(header_num);
    close_page(header_num);

   printf("Failed to open %s\n", pathname);
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

int pick_up_page(int i){
	int tmp, tmp2;
	tmp = buf_manager.frames[i].next_lru ; // 옮기려고 하는 것의 next lru 
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
	else buf_manager.frames[buf_manager.frames[i].prev_lru].next_lru = tmp;	// 위치를 가장 뒤로 옮기려고 하는 것의 lru
	
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

int close_page(int buf_num){
	uframe.is_pinned = 0;
	return  buf_num;
}

int set_dirty(int buf_num){
	uframe.is_dirty = 1;
	return buf_num;
}

void drop_page(int buf_num){
	if(uframe.is_dirty){
		file_write_page(uframe.table_id,uframe.page_num,&uframe.page);
	}
	uframe.table_id = 0;
	uframe.page_num=0;
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
    char * r = find(table_id, key);
    if (r == NULL)
        printf("Record not found under key %lld.\n", key);
    else 
        printf("Record -- key %lld, value %c.\n",
                 key, r);
    free(r);
}//done do not need

pagenum_t find_leaf(int table_id, int64_t key ) {
    int i = 0;
    int temp1, temp2;
    int buf_num;
	int header_num = open_page(table_id,0);

	int pagenum = off_to_num(hpage.root_off);
	
	if(pagenum==0){
		close_page(header_num);
		return 0;
	}
	
	buf_num = open_page(table_id,pagenum);
	temp2 = pagenum; 
    while (!upage.is_leaf) {
        i = -1;
        while (i <= upage.num_keys) {
            if (key >= upage.branches[i+1].key && i<upage.num_keys-1) {
				i++;
            }
            else break;
        }
        if (i == upage.num_keys){
        	close_page(buf_num);
        	close_page(header_num);
        	return 0;	
        }
        else if(i==-1)
        	temp1 = off_to_num(upage.ex_off);
        else
        	temp1 = off_to_num(upage.branches[i].child_off);
        close_page(buf_num);
        buf_num = open_page(table_id,temp1);
        temp2 = temp1;
    }
	
	close_page(header_num);
	close_page(buf_num);
    return temp2;
}


char * find(int table_id, int64_t key) {

    int i = 0;
    int buf_num;
    pagenum_t pagenum;

    pagenum = find_leaf( table_id, key );

    if (pagenum == 0) return NULL;
    
    buf_num = open_page(table_id,pagenum);
    	
    	
    for (i = 0; i < upage.num_keys; i++)
        if (upage.records[i].key == key) break;
    
    if (i == upage.num_keys) {
    	close_page(buf_num);
    	return NULL;
    }
    else{
    	char *a;
    	a = upage.records[i].value;
    	close_page(buf_num);
    	return (char *)a;
    }
}


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
record * make_record(int64_t key,char value[120]) {
	if(strlen(value)>120){
		printf("Value must be shorter than 120 bytes");
		return 0;
	}
    record * new_record = (record *)calloc(1,sizeof(record));
    if (new_record == NULL) {
        perror("Record creation.");
        exit(EXIT_FAILURE);
    }
    else {
        strcpy(new_record->value, value);
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

pagenum_t insert_into_leaf( int table_id, pagenum_t leaf, int64_t key, char * value ) {

	int buf_num = open_page(table_id,leaf);
	
    int i, insertion_point;

    insertion_point = 0;
    while (insertion_point < upage.num_keys && upage.records[insertion_point].key < key)
        insertion_point++;

    for (i = upage.num_keys; i > insertion_point; i--) {
    	strcpy(upage.records[i].value,upage.records[i-1].value);
    	upage.records[i].key = upage.records[i-1].key;
    }
    
    strcpy(upage.records[insertion_point].value,value);
    upage.records[insertion_point].key = key;
    upage.num_keys++;
    
    set_dirty(buf_num);
    close_page(buf_num);
    
    return leaf;
}//buffer D

pagenum_t insert_into_leaf_after_splitting(int table_id, pagenum_t leaf, int64_t key , char inserted_value[120]) {

	int buf_num = open_page(table_id, leaf);

	pagenum_t new_pagenum = make_leaf(table_id);
	
	int buf_num2 = open_page(table_id,new_pagenum);
	
    int insertion_index, split, i, j;
    int64_t new_key;

	int64_t temp_keys[order];
	char temp_value[order][120];

    insertion_index = 0;
    while (insertion_index < order - 1 && upage.records[insertion_index].key < key)
        insertion_index++;

    for (i = 0, j = 0; i < upage.num_keys; i++, j++) {
        if (j == insertion_index) j++;
        strcpy(temp_value[j],upage.records[i].value);
        temp_keys[j] = upage.records[i].key;
    }

    
    strcpy(temp_value[insertion_index],inserted_value);
    temp_keys[insertion_index] = key;

    upage.num_keys = 0;

    split = cut(order - 1);

    for (i = 0; i < split; i++) {
    	strcpy(upage.records[i].value,temp_value[i]);
	    upage.records[i].key = temp_keys[i];
        upage.num_keys++;
    }

    for (i = split, j = 0; i < order; i++, j++) {
     	strcpy(uupage.records[j].value,temp_value[i]);
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

pagenum_t start_new_tree(int table_id,int64_t key, char * value) {
	
    pagenum_t root = make_leaf(table_id);
	int root_num = open_page(table_id,root);
	int header_num = open_page(table_id,0);
	
	hpage.root_off = num_to_off(root);
	set_dirty(header_num);
		
	strcpy(rpage.records[0].value,value);
	rpage.records[0].key = key;
	rpage.parent=0;
	rpage.num_keys++;
	set_dirty(root_num);
	
	close_page(header_num);
	close_page(root_num);

    return root;
}

int insert(int table_id,int64_t key, char value[120] ) {

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
        return start_new_tree(table_id,key, value);
        
	int root_num = open_page(table_id,root);


    leaf = find_leaf(table_id, key);
    buf_num = open_page(table_id,leaf);

    if (upage.num_keys < order - 1) {
    	close_page(buf_num);
        leaf = insert_into_leaf(table_id,leaf, key, value);
        
        return leaf;
    }

     close_page(buf_num);

    return insert_into_leaf_after_splitting(table_id,leaf, key, value);
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


pagenum_t remove_entry_from_node(int table_id, pagenum_t n, int64_t key, char value[120]) {

    int i, num_pointers;
	
	int buf_num = open_page(table_id,n);
	
    // Remove the key and shift other keys accordingly.
    i = 0;
    if(upage.is_leaf){
	    while (upage.records[i].key != key)
	        i++;
	    for (++i; i < upage.num_keys; i++){
	        upage.records[i - 1].key = upage.records[i].key;
	        strcpy(upage.records[i-1].value,upage.records[i].value);}
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
            upage.records[i].value[0]='\0';
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
            strcpy(npage.records[i].value,upage.records[j].value);
            npage.num_keys++;
        }
        strcpy(npage.records[order-1].value,upage.records[order-1].value);
    }
    set_dirty(buf_num);
    set_dirty(neighbor_num);
	
    root = delete_entry(table_id, off_to_num(upage.parent), k_prime, upage.records[0].value);
    
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
            strcpy(upage.records[i].value, upage.records[i - 1].value);
        	}
        	strcpy(upage.records[0].value, npage.records[npage.num_keys - 1].value);
            npage.records[npage.num_keys-1].value[0]='\0';
            upage.records[0].key = npage.records[npage.num_keys - 1].key;
            parent_num = open_page(table_id,off_to_num(upage.parent));
            ppage.records[k_prime_index].key = upage.records[0].key;
        }
    }

    else {  
        if (upage.is_leaf) {
            upage.records[upage.num_keys].key = npage.records[0].key;
            strcpy(upage.records[upage.num_keys].value,npage.records[0].value);
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
            strcpy(npage.records[i].value,npage.records[i+1].value);
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

pagenum_t delete_entry(int table_id, pagenum_t n, int64_t key, char value[120] ) {

    int min_keys;
    pagenum_t neighbor;
	int header_num = open_page(table_id,0);
	int root_num = open_page(table_id,off_to_num(hpage.root_off));
	
    page_t * neighborP = calloc(1,PAGESIZE); 
    int neighbor_index;
    int k_prime_index, k_prime;
    int capacity;
	page_t* parentP = calloc(1,PAGESIZE);

	page_t * nP = calloc(1,PAGESIZE);
	
    n = remove_entry_from_node(table_id,n, key, value);

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

pagenum_t delete(int table_id, int key) {

    int64_t key_leaf;
    char * key_value = find(table_id,key);


    key_leaf = find_leaf(table_id,key);
    if (key_value != NULL && key_leaf != NULL) {
        key_leaf = delete_entry(table_id, key_leaf, key, key_value);
    }
    return key_leaf;
}
