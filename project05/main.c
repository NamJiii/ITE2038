#include "bpt.c"


int main(int argc, char ** argv) {
	
   int64_t input;
   char instruction;
   char *buf;
   buf = malloc(120);
   int i;
	page_t * c = calloc(1,PAGESIZE);
   
   open_db("test.db");
   while(scanf("%c", &instruction) != EOF){
      switch(instruction){
         case 'i':
            scanf("%"PRId64" %s", &input, buf);
            insert(input, buf);
            break;
         case 'f':
            scanf("%"PRId64, &input);
            buf = find(input);
            if (buf) {
               printf("Key: %"PRId64", Value: %s\n", input, buf);
            } else{
            	buf = malloc(120);
            	printf("Not Exists\n");
            }
            fflush(stdout);
            break;
		case 't':
			print_tree();

            break;
         case 'd':
            scanf("%"PRId64, &input);
            delete(input);
            break;
         case 'q':
            while(getchar() != (int)'\n');
            return EXIT_SUCCESS;
            break;
      }
      while (getchar() != (int)'\n');
   }
   printf("\n");
   return EXIT_SUCCESS;
}

