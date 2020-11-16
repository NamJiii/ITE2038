#include "bpt.h"

// INSERTION
 
header_page* headerP;
page_t* rootP;
queue* q;

 int internal_order = 31;//248
 int order = 31; // 32?
 
 
 int db_fd;

int open_db(char *pathname) {
   int temp;
   db_fd = open(pathname, O_RDWR | O_CREAT | O_EXCL | O_SYNC, 0777);
   headerP = (header_page*)calloc(1, PAGESIZE);

   if (db_fd > 0) {
      headerP->num_pages = 1;
      temp = pwrite(db_fd, headerP, PAGESIZE, 0);
      if (temp < PAGESIZE) {
         printf("Failed to write header_page\n");
         exit(EXIT_FAILURE);
      }

      return 0;
   }

   db_fd = open(pathname, O_RDWR | O_SYNC);
   if (db_fd > 0) {
      temp = pread(db_fd, headerP, PAGESIZE, 0); 
      if (temp < PAGESIZE) {
         printf("Failed to read header_page\n");
         exit(EXIT_FAILURE);
      }
      if (headerP->root_off != 0) {
         rootP = (page_t *)calloc(1, PAGESIZE);
         temp = pread(db_fd, rootP, PAGESIZE, headerP->root_off);
         if (temp < PAGESIZE) {
            printf("%"PRId64"\n", headerP->root_off);
            printf("Failed to read root_page\n");
            printf("%s\n", strerror(errno));
            exit(EXIT_FAILURE);
         }
      }
      else {
         printf("DB is empty\n"); 
      }
      return 0;
   } 

   printf("Failed to open %s\n", pathname);
   return -1;
}

off_t num_to_off(pagenum_t pagenum){
	return PAGESIZE*pagenum;
}

pagenum_t off_to_num(off_t off){
	return off/PAGESIZE;
}

pagenum_t file_alloc_page(){
	int i;
	page_t* c;
	file_read_page(0,(page_t*)headerP);
	int tmp = headerP->num_pages;
	for(i=9; i>=0; i--){
		c = (page_t * ) calloc(1,PAGESIZE);

		c->parent=headerP->free_off;//이때 parent 는 그냥 next free page 
		headerP->free_off = num_to_off(tmp+i);
		file_write_page(tmp+i,c);
	}
	headerP->num_pages = tmp+10;
	file_write_page(0,(page_t*)headerP);
	free(c);
	return tmp;
}

void file_free_page(pagenum_t pagenum){
	page_t* c =calloc(1,PAGESIZE);
	file_read_page(0,(page_t*)headerP);
	c->parent = headerP->free_off;
	headerP->free_off = num_to_off(pagenum);
	headerP->num_pages -- ;
	file_write_page(pagenum,c);
	file_write_page(0,(page_t*)headerP);
	
	free(c);
}

void file_read_page(pagenum_t pagenum, page_t* dest){
	int temp;
	temp = pread(db_fd, dest, PAGESIZE, num_to_off(pagenum)); 
	if (temp<PAGESIZE) printf("read fail\n");
}

void file_write_page(pagenum_t pagenum, const page_t* src){
	int temp;
	temp = pwrite(db_fd, src, PAGESIZE, num_to_off(pagenum));
}

//FIND

void find_and_print(int64_t key) {
    char * r = find(key);
    if (r == NULL)
        printf("Record not found under key %lld.\n", key);
    else 
        printf("Record -- key %lld, value %c.\n",
                 key, r);
    free(r);
}//done do not need

pagenum_t find_leaf(int64_t key ) {
    int i = 0;
    off_t temp;
	page_t * c = (page_t *)calloc(1,PAGESIZE);
	temp = off_to_num(headerP->root_off);
    file_read_page(temp, c);
    if (temp == 0){
    	free(c);
    	return 0;
    } 

    while (!c->is_leaf) {
        i = -1;
        while (i <= c->num_keys) {
            if (key >= c->branches[i+1].key && i<c->num_keys-1) {
				i++;
            }
            else break;
        }
        if (i == c->num_keys){
        	free(c);
        	return 0;}
        else if(i==-1)
        	temp = off_to_num(c->ex_off);
        else
        temp = off_to_num(c->branches[i].child_off);
        file_read_page(temp,c);
    }
    free(c);

    return temp;
}


char * find(int64_t key) {

    int i = 0;
    pagenum_t pagenum;

    pagenum = find_leaf( key );

    page_t * c =(page_t *) calloc(1,PAGESIZE);

    file_read_page(pagenum,c);

    if (pagenum == 0) return NULL;
    for (i = 0; i < c->num_keys; i++)
        if (c->records[i].key == key) break;
    
    if (i == c->num_keys) {
    	free(c);
    	return NULL;
    }
    else{
    	char *a;
    	a = c->records[i].value;
    	free(c);
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

pagenum_t make_node( void ) {
    
    pagenum_t pagenum;
    
    if(headerP->free_off==0)pagenum = file_alloc_page();
	
	else{
		off_t temp = headerP->free_off;
		pagenum = off_to_num(temp);
	}
	page_t* tempP =  (page_t *) calloc(1,PAGESIZE);
	file_read_page(pagenum,tempP);

	if (tempP == NULL) {
    perror("Node creation.");
    exit(EXIT_FAILURE);
    }
				
	headerP->free_off = tempP->parent;
	tempP->parent = 0;
	file_write_page(0,(page_t*)headerP);
		
	file_write_page(pagenum,tempP);
	free(tempP);
	return pagenum;
}

pagenum_t make_leaf( void ) {
    pagenum_t pagenum = make_node();
    page_t * c = (page_t *) calloc(1,PAGESIZE);
    file_read_page(pagenum,c);
    c->is_leaf = true;
    file_write_page(pagenum,c);
    free(c);
    return pagenum;
}

int get_left_index(pagenum_t parent, pagenum_t left) {

    int left_index = -1;
    
    page_t * parentP = (page_t *) calloc(1,PAGESIZE);
    page_t * leftP = (page_t *) calloc(1,PAGESIZE);
    file_read_page(parent,parentP);
    file_read_page(left,leftP);
    
    while (left_index <= parentP->num_keys && 
            parentP->branches[left_index].child_off != num_to_off(left))
        left_index++;
    free(parentP);
    free(leftP);
    return left_index;
}

pagenum_t insert_into_leaf( pagenum_t leaf, int64_t key, char * value ) {

	page_t * leafP = (page_t *) calloc(1,PAGESIZE);
	file_read_page(leaf,leafP);
	
    int i, insertion_point;

    insertion_point = 0;
    while (insertion_point < leafP->num_keys && leafP->records[insertion_point].key < key)
        insertion_point++;

    for (i = leafP->num_keys; i > insertion_point; i--) {
    	strcpy(leafP->records[i].value,leafP->records[i-1].value);
    	leafP->records[i].key = leafP->records[i-1].key;
    }
    
    strcpy(leafP->records[insertion_point].value,value);
    leafP->records[insertion_point].key = key;
    leafP->num_keys++;
    file_write_page(leaf,leafP);
    free(leafP);
    return leaf;
}

pagenum_t insert_into_leaf_after_splitting(pagenum_t leaf, int64_t key , char inserted_value[120]) {

	page_t * leafP = (page_t *) calloc(1,PAGESIZE);
	file_read_page(leaf,leafP);
	pagenum_t new_pagenum = make_leaf();
    page_t * new_leaf = (page_t *) calloc(1,PAGESIZE);
	file_read_page(new_pagenum,new_leaf);
	
    int insertion_index, split, i, j;
    int64_t new_key;

	int64_t temp_keys[order];
	char temp_value[order][120];

    insertion_index = 0;
    while (insertion_index < order - 1 && leafP->records[insertion_index].key < key)
        insertion_index++;

    for (i = 0, j = 0; i < leafP->num_keys; i++, j++) {
        if (j == insertion_index) j++;
        strcpy(temp_value[j],leafP->records[i].value);
        temp_keys[j] = leafP->records[i].key;
    }

    
    strcpy(temp_value[insertion_index],inserted_value);
    temp_keys[insertion_index] = key;

    leafP->num_keys = 0;

    split = cut(order - 1);

    for (i = 0; i < split; i++) {
    	strcpy(leafP->records[i].value,temp_value[i]);
	    leafP->records[i].key = temp_keys[i];
        leafP->num_keys++;
    }

    for (i = split, j = 0; i < order; i++, j++) {
     	strcpy(new_leaf->records[j].value,temp_value[i]);
	    new_leaf->records[j].key = temp_keys[i];
        new_leaf->num_keys++;
    }

    new_leaf->ex_off = leafP->ex_off;
    leafP->ex_off = num_to_off(new_pagenum);

    for (i = leafP->num_keys; i < order - 1; i++){
        leafP->records[i].key = 0;}

    for (i = new_leaf->num_keys; i < order - 1; i++){
        new_leaf->records[i].key = 0;
	}
		 
    new_leaf->parent = leafP->parent;
    new_key = new_leaf->records[0].key;
    
    file_write_page(leaf,leafP);
    file_write_page(new_pagenum,new_leaf);
    free(leafP);
    free(new_leaf);

    return insert_into_parent(leaf, new_key, new_pagenum);
}

pagenum_t insert_into_node(pagenum_t n, int left_index, int64_t key, pagenum_t right) {
    int i, temp;
	
	file_read_page(0,(page_t*)headerP);

	page_t* nP = (page_t*) calloc(1,PAGESIZE);
	file_read_page(n,nP);
	
	page_t* rightP = (page_t*) calloc(1,PAGESIZE);
	file_read_page(right,rightP);
	
    for (i = nP->num_keys; i > left_index; i--) {
    	nP->branches[i+1].key = nP->branches[i].key;
        nP->branches[i+1].child_off = nP->branches[i].child_off; //
    }
    nP->branches[left_index + 1].child_off = num_to_off(right);
    nP->branches[left_index+1].key = key;
    nP->num_keys++;
    
    file_write_page(n,nP);
    
    free(nP);
    free(rightP);
    
    return n;
}

pagenum_t insert_into_node_after_splitting(pagenum_t old_node, int left_index, 
												int64_t key, pagenum_t right) {

	file_read_page(0,(page_t*)headerP);
	
    int i, j, split, k_prime;
    page_t * new_nodeP = (page_t *) calloc(1,PAGESIZE);
    pagenum_t new_node;
	page_t * child = (page_t *) calloc(1,PAGESIZE);
    
	int64_t temp_keys[internal_order];
	off_t temp_off[internal_order];
	
	file_read_page(off_to_num(headerP->root_off),rootP);
	
	page_t * old_nodeP = (page_t *) calloc(1,PAGESIZE);
	file_read_page(old_node,old_nodeP);
	
	page_t * rightP = (page_t *) calloc(1,PAGESIZE);
	file_read_page(right,rightP);

    for (i = 0, j = 0; i < old_nodeP->num_keys; i++, j++) {
		if (j == left_index + 1) j++;
		temp_off[j] = old_nodeP->branches[i].child_off;
	}

	for (i = 0, j = 0; i < old_nodeP->num_keys; i++, j++) {
		if (j == left_index + 1) j++;
		temp_keys[j] = old_nodeP->branches[i].key;
	}

	temp_keys[left_index + 1] = key;
	temp_off[left_index + 1] = num_to_off(right);
 
    split = cut(internal_order-1);
    new_node = make_node();
    file_read_page(new_node,new_nodeP);
    
    old_nodeP->num_keys = 0;
    
    for (i = 0; i < split; i++) {
        old_nodeP->branches[i].key = temp_keys[i];
        old_nodeP->branches[i].child_off = temp_off[i];
        old_nodeP->num_keys++;
    }
        
    new_nodeP->ex_off = temp_off[split];
	k_prime = temp_keys[split];
	new_nodeP->num_keys = 0;

	for (i = split + 1, j = 0; i < internal_order; i++, j++) {
		new_nodeP->branches[j].child_off = temp_off[i];
		new_nodeP->branches[j].key = temp_keys[i];
		new_nodeP->num_keys++;
	}
    
    new_nodeP->parent = old_nodeP->parent;
    for (i = 0; i <= new_nodeP->num_keys; i++) {
    	file_read_page(off_to_num(new_nodeP->branches[i].child_off),child);
        child->parent = num_to_off(new_node);
        file_write_page(off_to_num(new_nodeP->branches[i].child_off),child);
    }
    file_write_page(new_node,new_nodeP);
    file_write_page(old_node,old_nodeP);
    file_write_page(right,rightP);
    
    free(new_nodeP);
    free(child);
    free(old_nodeP);
    free(rightP);

    return insert_into_parent(old_node, k_prime, new_node);
}

pagenum_t insert_into_parent(pagenum_t left, int64_t key, pagenum_t right) {
    int left_index;

	page_t * leftP = (page_t *) calloc(1,PAGESIZE);
	file_read_page(left,leftP);
	
	page_t * rightP = (page_t *) calloc(1,PAGESIZE);
	file_read_page(right,rightP);
	
    page_t * parent = (page_t *) calloc(1,PAGESIZE);

    file_read_page(off_to_num(leftP->parent),parent);
    /* Case: new root. */

    if (leftP->parent == 0){
    	free(leftP);
    	free(rightP);
    	free(parent);
        return insert_into_new_root(left, key, right);
    }
    
    left_index = get_left_index(off_to_num(leftP->parent), left);

     int64_t tmp =parent->num_keys;
     off_t parentoff = off_to_num(leftP->parent);
     free(leftP);
     free(rightP);
     free(parent);

    if (tmp < internal_order - 1)
        return insert_into_node(parentoff, left_index, key, right);

    return insert_into_node_after_splitting(parentoff, left_index, key, right);
}

pagenum_t insert_into_new_root(pagenum_t left, int64_t key, pagenum_t right) {

	page_t* leftP = (page_t *) calloc(1,PAGESIZE);
	file_read_page(left,leftP);

	page_t* rightP = (page_t *) calloc(1,PAGESIZE);
	file_read_page(right,rightP);

    pagenum_t root = make_node();
	rootP = calloc(1,PAGESIZE);
    file_read_page(root,rootP);

    rootP->ex_off= num_to_off(left);
    rootP->branches[0].key = key; // minimum value;
    rootP->branches[0].child_off = num_to_off(right);
    rootP->num_keys++;
    rootP->parent = 0;

    leftP->parent = num_to_off(root);
    rightP->parent = num_to_off(root);
    headerP->root_off = num_to_off(root);
    
    file_write_page(0,(page_t*)headerP);
    file_write_page(root,rootP);
    file_write_page(right,rightP);
    file_write_page(left,leftP);
    
    free(leftP);
    free(rightP);
    return root;
}

pagenum_t start_new_tree(int64_t key, char * value) {
	
    pagenum_t root = make_leaf();

	rootP = (page_t*)calloc(1,PAGESIZE);
	file_read_page(0,(page_t *)headerP);
	headerP->root_off = num_to_off(root);
	file_write_page(0,(page_t*)headerP);
	file_read_page(root,rootP);	
	strcpy(rootP->records[0].value,value);
	rootP->records[0].key = key;
	rootP->parent=0;
	rootP->num_keys++;
    file_write_page(root,rootP);
    file_write_page(0,(page_t *)headerP);

    return root;
}

pagenum_t insert(int64_t key, char value[120] ) {

    page_t * leafP = calloc(1,PAGESIZE);
    pagenum_t leaf;

	int root = off_to_num(headerP->root_off);
	file_read_page(root,rootP);

    if (find(key) != NULL){
    	free(leafP);
    	return 0;
    }


    if (rootP == NULL) 
        return start_new_tree(key, value);

    leaf = find_leaf(key);
	file_read_page(leaf,leafP);

    if (leafP->num_keys < order - 1) {
        leaf = insert_into_leaf(leaf, key, value);
        free(leafP);
        return leaf;
    }

     free(leafP);

    return insert_into_leaf_after_splitting(leaf, key, value);
}

void enqueue( pagenum_t pagenum ) {
    queue * enq = (queue *)malloc(sizeof(queue));
    queue * tmp;
    enq->pnum = pagenum;
    enq->next = NULL;

    // q is empty
    if ( q == NULL ) q = enq;
    
    // q has page
    else {
        tmp = q;
        while( tmp->next != NULL ) 
            tmp = tmp->next;
        tmp->next = enq;
    }
}
pagenum_t dequeue( void ) {
    queue * n = q;
    pagenum_t rt = n->pnum;
    q = q->next;
    free(n);
    return rt;
}

void print_tree( void ) {

    page_t * n;
    page_t * par_n;
    int i = 0;
    int rank = 0;
    int new_rank = 0;
    pagenum_t nnum, rnum;
    
    if (headerP->root_off == 0) {
        printf("Empty tree.\n");
        return;
    }

    n = (page_t *)calloc(1, PAGESIZE);
    par_n = (page_t *)calloc(1, PAGESIZE);
    q = (queue*)malloc(sizeof(queue));
    q = NULL;
    rnum = off_to_num(headerP->root_off); 
    enqueue(rnum); 
    while( q != NULL ) {
        nnum = dequeue();
        file_read_page(nnum, n); 
        for(i=0; i < n->num_keys; ++i) { 
            printf("%"PRId64" ", n->is_leaf?n->records[i].key:n->branches[i].key);
        }
        if (!n->is_leaf) {
            for (i = -1; i <= n->num_keys; ++i)
                enqueue(off_to_num(n->branches[i].child_off));
        }
        printf("| ");
    }
    printf("\n");

    free(n);
    free(par_n);
    free(q);
}

// DELETION.

int get_neighbor_index( pagenum_t n ) {

    int i;

	page_t* nP = calloc(1,PAGESIZE);
	file_read_page(n,nP);
	page_t* parentP = calloc(1,PAGESIZE);
	file_read_page(off_to_num(nP->parent),parentP);
	
    for (i = -1; i <= parentP->num_keys; i++)
        if (parentP->branches[i].child_off == num_to_off(n)){
        	free(nP);
        	free(parentP);
        	return i - 1;
		}
            
    // Error state.
    printf("Search for nonexistent pointer to node in parent.\n");
    printf("Node:  %#lx\n", (unsigned long)n);
    exit(EXIT_FAILURE);
}


pagenum_t remove_entry_from_node(pagenum_t n, int64_t key, char value[120]) {

    int i, num_pointers;
	
	page_t * nP = calloc(1,PAGESIZE);
	file_read_page(n,nP);
    // Remove the key and shift other keys accordingly.
    i = 0;
    if(nP->is_leaf){
	    while (nP->records[i].key != key)
	        i++;
	    for (++i; i < nP->num_keys; i++){
	        nP->records[i - 1].key = nP->records[i].key;
	        strcpy(nP->records[i-1].value,nP->records[i].value);}
    }
    
    else{
	    while (nP->branches[i].key != key)
	        i++;
	    for (++i; i < nP->num_keys; i++){
	        nP->branches[i - 1].key = nP->branches[i].key;
	        nP->branches[i - 1].child_off = nP->branches[i].child_off; } 	
    }

    nP->num_keys--;

    if (nP->is_leaf)
        for (i = nP->num_keys; i < internal_order - 1; i++)
            nP->records[i].value[0]='\0';
    else
        for (i = nP->num_keys + 1; i < internal_order; i++)
            nP->branches[i].child_off = 0; 
            
    file_write_page(n,nP);
	free(nP);
    return n;
}


pagenum_t adjust_root() {
	page_t* old_root = calloc(1,PAGESIZE);
	file_read_page(off_to_num(headerP->root_off),old_root);


    if (old_root->num_keys > 0){
    	free(old_root);
        return off_to_num(headerP->root_off);
	}
	
    off_t new_root_off;
	page_t* new_root = calloc(1,PAGESIZE);
	
    if (!old_root->is_leaf) {
    	new_root_off = old_root->branches[-1].child_off;
        file_read_page(off_to_num(new_root_off),new_root);
        new_root->parent = 0;
    	file_write_page(off_to_num(new_root_off),new_root);
    }


    else new_root_off = 0;
    headerP->root_off = new_root_off;
    file_write_page(0,headerP);
    file_read_page(off_to_num(new_root_off),rootP);

    free(new_root);
    free(old_root);

    return new_root;
}



pagenum_t coalesce_nodes(pagenum_t n, pagenum_t neighbor, int neighbor_index, int k_prime) {
    int i, j, neighbor_insertion_index, n_end;
    pagenum_t root;
	page_t * nP = calloc(1,PAGESIZE);
	file_read_page(n,nP);
	page_t *neighborP = calloc(1,PAGESIZE);
	file_read_page(neighbor,neighborP);
	pagenum_t temp;
	page_t* tempP = calloc(1,PAGESIZE);

    if (neighbor_index == -2) {
    	file_write_page(neighbor,nP);
    	file_write_page(n,neighborP);
    	file_read_page(neighbor,neighborP);
    	file_read_page(n,nP);
    }

    neighbor_insertion_index = neighborP->num_keys;
    
    if (!nP->is_leaf) {

        neighborP->branches[neighbor_insertion_index/*-1*/].key = k_prime; 
        neighborP->num_keys++;


        n_end = nP->num_keys;

        for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++) {
            neighborP->branches[i].key = nP->branches[j].key;
            neighborP->branches[i].child_off = nP->branches[j].child_off;
            neighborP->num_keys++;
            nP->num_keys--;
        }

        neighborP->branches[i].child_off = nP->branches[j].child_off;


        for (i = 0; i < neighborP->num_keys + 1; i++) {
            temp = neighborP->branches[i].child_off;
            file_read_page(temp,tempP);
            tempP->parent = num_to_off(neighbor);
            file_write_page(temp,tempP);
        }
    }


    else {
        for (i = neighbor_insertion_index, j = 0; j < nP->num_keys; i++, j++) {
            neighborP->records[i].key = nP->records[j].key;
            strcpy(neighborP->records[i].value,nP->records[j].value);
            neighborP->num_keys++;
        }
        strcpy(neighborP->records[order-1].value,nP->records[order-1].value);
    }
	file_write_page(n,nP);
	file_write_page(neighbor,neighborP);
	
    root = delete_entry(off_to_num(nP->parent), k_prime/*, nP->value*/);
    free(nP);
    free(neighborP);
    free(tempP);
    return root;
}


pagenum_t redistribute_nodes(pagenum_t n, pagenum_t neighbor, int neighbor_index, 
        int k_prime_index, int k_prime) {  

	page_t* nP = calloc(1,PAGESIZE);
	file_read_page(n,nP);
	page_t* neighborP = calloc(1,PAGESIZE);
	file_read_page(neighbor,neighborP);
	
    int i;
    int tmp;
    page_t * tmpP = calloc(1,PAGESIZE);
	page_t * parentP = calloc(1,PAGESIZE);

    if (neighbor_index != -2) {


        if (!nP->is_leaf) {
        	for (i = nP->num_keys; i > 0; i--) {
            nP->branches[i].key = nP->branches[i - 1].key;
            nP->branches[i].child_off = nP->branches[i - 1].child_off;
       		 }
			nP->branches[0].child_off = nP->ex_off;
            nP->branches[-1].child_off = neighborP->branches[neighborP->num_keys].child_off;
            tmp = nP->branches[-1].child_off;
            file_read_page(tmp,tmpP);
            tmpP->parent = num_to_off(n);
            neighborP->branches[neighborP->num_keys].child_off = 0;
            nP->branches[0].key = k_prime;
            file_read_page(off_to_num(nP->parent),parentP);
            parentP->branches[k_prime_index].key = neighborP->records[neighborP->num_keys].key;
        }
        else {
        	for (i = nP->num_keys; i > 0; i--) {
            nP->records[i].key = nP->records[i - 1].key;
            strcpy(nP->records[i].value, nP->records[i - 1].value);
        	}
        	strcpy(nP->records[0].value, neighborP->records[neighborP->num_keys - 1].value);
            neighborP->records[neighborP->num_keys-1].value[0]='\0';
            nP->records[0].key = neighborP->records[neighborP->num_keys - 1].key;
            file_read_page(off_to_num(nP->parent),parentP);
            parentP->records[k_prime_index].key = nP->records[0].key;
        }
    }

    else {  
        if (nP->is_leaf) {
            nP->records[nP->num_keys].key = neighborP->records[0].key;
            strcpy(nP->records[nP->num_keys].value,neighborP->records[0].value);
			file_read_page(off_to_num(nP->parent),parentP);
            parentP->records[k_prime_index].key = neighborP->records[1].key;
        }
        else {
            nP->branches[nP->num_keys].key = k_prime;
            nP->branches[nP->num_keys + 1].child_off = neighborP->branches[0].child_off;
            tmp = nP->branches[nP->num_keys + 1].child_off;

            file_read_page(off_to_num(tmp),tmpP);
            file_read_page(off_to_num(tmpP->parent),parentP);
            file_read_page(off_to_num(parentP->parent),parentP);
            parentP->branches[k_prime_index].key = neighborP->branches[0].key;
        }
        for (i = 0; i < neighborP->num_keys - 1; i++) { 
            neighborP->records[i].key = neighborP->records[i + 1].key;
            strcpy(neighborP->records[i].value,neighborP->records[i+1].value);
        }
        if (!nP->is_leaf)
			neighborP->branches[i].child_off = neighborP->branches[i + 1].child_off;
    }

    nP->num_keys++;
    neighborP->num_keys--;

	file_write_page(n,nP);
	file_write_page(neighbor,neighborP);
	file_write_page(off_to_num(nP->parent),parentP);
	
	free(nP);
	free(neighborP);
	free(tmpP);
	free(parentP);
	
    return off_to_num(headerP->root_off);
}

pagenum_t delete_entry(pagenum_t n, int64_t key, char value[120] ) {

    int min_keys;
    pagenum_t neighbor;
    page_t * neighborP = calloc(1,PAGESIZE); 
    int neighbor_index;
    int k_prime_index, k_prime;
    int capacity;
	page_t* parentP = calloc(1,PAGESIZE);

	page_t * nP = calloc(1,PAGESIZE);
    n = remove_entry_from_node(n, key, value);

    
print_tree();
    if (n == off_to_num(headerP->root_off)) 
        return adjust_root(rootP);

	file_read_page(n,nP);

    min_keys = nP->is_leaf ? (cut(order - 1))/2 : (cut(internal_order) - 1)/2;


    if (nP->num_keys >= min_keys)
        return off_to_num(headerP->root_off);

    neighbor_index = get_neighbor_index( n );
    k_prime_index = neighbor_index == -2 ? 0 : neighbor_index+1;
    file_read_page(off_to_num(nP->parent),parentP);
    k_prime = parentP->branches[k_prime_index].key;

    neighbor = neighbor_index == -2 ? off_to_num(parentP->branches[0].child_off) : 
        off_to_num(parentP->branches[neighbor_index].child_off);

    capacity = nP->is_leaf ? order : internal_order - 1;

    file_read_page(neighbor,neighborP);
    
    // Coalescence.
print_tree();
    if (neighborP->num_keys + nP->num_keys < capacity)
        return coalesce_nodes(n, neighbor, neighbor_index, k_prime);

    //Redistribution.

    else
        return redistribute_nodes(n, neighbor, neighbor_index, k_prime_index, k_prime);
}



//Master deletion function.

pagenum_t delete(int key) {

    int64_t key_leaf;
    char * key_value = find(key);


    key_leaf = find_leaf(key);
    if (key_value != NULL && key_leaf != NULL) {
        key_leaf = delete_entry(key_leaf, key, key_value);
    }
    return key_leaf;
}


