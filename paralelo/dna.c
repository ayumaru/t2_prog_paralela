#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// MAX char table (ASCII)
#define MAX 256

// Boyers-Moore-Hospool-Sunday algorithm for string matching
int bmhs(char *string, int n, char *substr, int m) {

	int d[MAX];
	int i, j, k;
	
	// pre-processing 
	for (j = 0; j < MAX; j++) 
		d[j] = m + 1;
	
	for (j = 0; j < m; j++)
		d[(int) substr[j]] = m - j;
	// searching
	i = m - 1;
	while (i < n) {
		k = i;
		j = m - 1;
		while ((j >= 0) && (string[k] == substr[j])) {
			j--;
			k--;
		}
		if (j < 0)	
		{
			return k + 1;
		}
		i = i + d[(int) string[i + 1]];
	}
	return -1;
}

FILE *fdatabase, *fquery, *fout;

void openfiles() {

	fdatabase = fopen("dna.in", "r+");
	if (fdatabase == NULL) {
		perror("dna.in");
		exit(EXIT_FAILURE);
	}

	fquery = fopen("query.in", "r");
	if (fquery == NULL) {
		perror("query.in");
		exit(EXIT_FAILURE);
	}

	fout = fopen("dna.out", "w");
	if (fout == NULL) {
		perror("fout");
		exit(EXIT_FAILURE);
	}

}

void closefiles() {
	fflush(fdatabase);
	fclose(fdatabase);

	fflush(fquery);
	fclose(fquery);

	fflush(fout);
	fclose(fout);
}

static inline void remove_eol(char *line) {

	int i = strlen(line) - 1;
	while (line[i] == '\n' || line[i] == '\r') {
		line[i] = 0;
		i--;
	}
}

typedef struct{
	char text_cab[100];
	char genoma[1000001];
	int tam;
} marcador;

marcador marcadores[1000];
int cabs;


void preprocessamento(){
	char linha[100];
	int j;
	fseek(fdatabase, 0, SEEK_SET);
	fgets(linha, 100, fdatabase);
	remove_eol(linha);

	strcpy( marcadores[0].text_cab, linha );
	j = 0;
	while(!feof(fdatabase))
	{
		fgets(linha, 100, fdatabase);
		remove_eol(linha);
		do { 
				strcat(marcadores[j].genoma, linha);
				
				if (fgets(linha, 100, fdatabase) == NULL)
					break;
				remove_eol(linha);
		} while (linha[0] != '>');
		marcadores[j].tam = strlen(marcadores[j].genoma); 
		cabs++;
		j++;
		strcpy( marcadores[j].text_cab, linha );
	}
	cabs-=1;
}

char *str;

int main(int argc, char** argv) {
	
	cabs=0;
	int nt;
	if ( argv[1] == NULL )
		nt = 1;
	else
		nt = atoi(argv[1]);
	
	str = (char*) malloc(sizeof(char) * 1000001);
	if (str == NULL) {
		perror("malloc str");
		exit(EXIT_FAILURE);
	}

	openfiles();

	preprocessamento();

	char desc_dna[100], desc_query[100];
	char line[100];
	int i,h, found, result;
	fgets(desc_query, 100, fquery); 
	remove_eol(desc_query);
	
	while (!feof(fquery)) {
		fprintf(fout, "%s\n", desc_query);
		// read query string
		fgets(line, 100, fquery);  
		remove_eol(line);
		str[0] = 0;
		i = 0;
		do { 
			strcat(str, line); 
			if (fgets(line, 100, fquery) == NULL)
				break;
			remove_eol(line);
		} while (line[0] != '>');
		strcpy(desc_query, line); 

		// read database and search
		found = 0;
		fseek(fdatabase, 0, SEEK_SET);
		fgets(line, 100, fdatabase);
		remove_eol(line);
	
		for(int y=0; y <= cabs; y=y+1)
		{	
			result = bmhs( marcadores[y].genoma, marcadores[y].tam, str, strlen(str)); 
			if (result > 0) {
				fprintf(fout, "%s\n%d\n", marcadores[y].text_cab, result);
				found++;
			}
		}
		
		if (!found)
			fprintf(fout, "NOT FOUND\n");
	}

	closefiles();

	free(str);
	return EXIT_SUCCESS;
}