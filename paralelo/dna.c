#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
// MAX char table (ASCII)
#define MAX 256
#define STD_TAG 0

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

	fdatabase = fopen("dna.in", "r+\n");
	if (fdatabase == NULL) {
		perror("dna.in\n");
		exit(EXIT_FAILURE);
	}

	fquery = fopen("query.in", "r\n");
	if (fquery == NULL) {
		perror("query.in\n");
		exit(EXIT_FAILURE);
	}

	fout = fopen("dna.out", "w\n");
	if (fout == NULL) {
		perror("fout\n");
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
	// int tam;
} marcador;

typedef struct{
	char line[100]; 
} cab_q;

typedef struct{
	int indice;
	int resposta;
	int found;
} acertos;

marcador marcadores[101];



int preprocessamento(int cabs){
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
		// marcadores[j].tam = strlen(marcadores[j].genoma); 
		cabs++;
		j++;
		strcpy( marcadores[j].text_cab, linha );
	}
	return cabs-1;	
}



char *str;

void envia_reconstroi(int rank, int n_proc, MPI_Status status, MPI_Datatype pixtype){ 
	// sai enviando o conjunto genoma + cabecalho + tamanho pra cada um dos processos
	for(int y=0; y <= 1; y=y+1)
	{
		//msg, tamanho da msg, tipo do dado, comunicador que ta enviando, comunicador que vai receber
		MPI_Bcast(&marcadores[y], 1, pixtype, 0, MPI_COMM_WORLD); 

	}
// ao final daqui todos os processos teriam uma copia dos marcadores
}
	


int main(int argc, char** argv) {
	
	int cabs=0;

	char desc_dna[100]; //, desc_query[100];
	char line[100];
	char msg[100];
	int i,h, found, result;
	int rank, n_proc;
	MPI_Status status;
	MPI_Datatype pixtype;
	MPI_Datatype rsp;
	int f = 0;
	int flageru = 0;

	//comeco da criacao dos processos
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &n_proc);
	
	str = (char*) malloc(sizeof(char) * 1000001);
	if (str == NULL) {
		perror("malloc str\n");
		exit(EXIT_FAILURE);
	}
	
	cab_q desc_query[n_proc];

	// so para verificacao e criacao do malloc
	// if (rank == 0) // talvez vale a pena fazer ele estatico
	// {
	// 	desc_query = malloc(n_proc*sizeof(cab_q));
	// 	if (desc_query == NULL)
	// 	{
	// 		perror("malloc desc\n");
	// 		f = 1;
	// 		for (int u=1; u<n_proc; u++)
	// 			MPI_Ssend(f, 1, MPI_INT, u, STD_TAG, MPI_COMM_WORLD);
	// 	}
	// }
	// else
	// 	MPI_Recv(f, 1, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
	
	// if (f == 1)
	// {
	// 	MPI_Finalize();
	// 	exit(EXIT_FAILURE);
	// }

	
	// criacao da struct 
	// int resultados[n_proc];
	acertos resultados[n_proc];
	int count = 2;
	int lengths[2] = {100,1000001};
	MPI_Datatype oldtypes[2] = {MPI_CHAR,MPI_CHAR};
	MPI_Aint offsets[2] = {0,100};

	
	MPI_Type_struct(count,lengths,offsets,oldtypes,&pixtype);
	MPI_Type_commit(&pixtype);
	// fim da criacao

	//outra criacao
	count = 3;
	int tams[3] = {sizeof(int),sizeof(int),sizeof(int)};
	MPI_Datatype n_oldtypes[3] = {MPI_INT,MPI_INT,MPI_INT};
	MPI_Aint n_offsets[3] = {sizeof(int),sizeof(int),sizeof(int)};
	MPI_Type_struct(count,tams,offsets,n_oldtypes,&rsp);
	MPI_Type_commit(&rsp);

	// so se for o processo 0
	if (rank == 0)
	{
		openfiles();
		cabs = preprocessamento(cabs);
		// isso vai ter que ser revisto depois, talvez na nova politica do while
		// fgets(desc_query, 100, fquery); 
		// remove_eol(desc_query);
		//printf("envio linha 226\n");
		for(int i=1; i<n_proc; i++)
			MPI_Ssend(&cabs, 1, MPI_INT, i, STD_TAG, MPI_COMM_WORLD);
	}
	else{
		// printf("Recebe linha 231\n");
		MPI_Recv(&cabs, 1, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
	}
	
	// printf("pre barreira 236\n");
	MPI_Barrier(MPI_COMM_WORLD);
	// termina aqui o if do processo 0
	envia_reconstroi(rank, n_proc, status, pixtype);
	
	//printf("rank: %d --- pre barreira 237\n", rank);
	MPI_Barrier(MPI_COMM_WORLD);
	// flageru de feof inicial

	if(rank == 0)
	{
		if(feof(fquery)) // se fim = true
		{
			flageru = 1;
		}
		//printf("envio 247\n");
		for(int i=1; i<n_proc; i++)
		{
			MPI_Ssend(&flageru, 1, MPI_INT, i, STD_TAG, MPI_COMM_WORLD);
		}
	}
	else{
		//printf("recv 255\n");
		MPI_Recv(&flageru, 1, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
	}
	//printf("flageru: %d, rank: %d\n", flageru, rank);

	while (flageru == 0){
		
		str[0] = 0;
		MPI_Barrier(MPI_COMM_WORLD);
		if (rank == 0)
		{
			if(feof(fquery)) // se fim = true
			{
				flageru = 1;
			}
			//printf("envio 269\n");
			for(int i=1; i<n_proc; i++)
			{
				MPI_Ssend(&flageru, 1, MPI_INT, i, STD_TAG, MPI_COMM_WORLD);
			}
			if (flageru == 0)
			{ 
				for (int q = 1; q < n_proc; q++ )
				{
						fgets(desc_query[q].line, 100, fquery); //rever a posicao de escrita disso
						remove_eol(desc_query[q].line);
						fprintf(fout, "%s\n", desc_query[q].line);
						// read query string
						fgets(line, 100, fquery);  
						remove_eol(line);
						i = 0;
						do { 
							strcat(str, line); 
							if (fgets(line, 100, fquery) == NULL)
								break;
							remove_eol(line);
						} while (line[0] != '>');
						strcpy(desc_query[q].line, line); 
						//printf("envio 291\n");
						MPI_Ssend(str, strlen(str)+1, MPI_CHAR, q, STD_TAG, MPI_COMM_WORLD);
				}
			}
		}
		else
		{
			//printf("recv 298\n");
			MPI_Recv(&flageru, 1, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			//printf("flageru: %d, rank: %d \n", flageru, rank);
			if (flageru == 0) 
			{
				//printf("recv 302\n");
				MPI_Recv(str, 1000001, MPI_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
				//printf("pos 302\n");
				resultados[0].found = 0;

				for(int y=0; y <= cabs; y=y+1)
				{	
					resultados[y].resposta = bmhs( marcadores[y].genoma, strlen(marcadores[y].genoma), str, strlen(str)); 
					if (resultados[y].resposta > 0) { 
						resultados[y].indice = y;
						resultados[0].found++;
					}
				}
			}
		}

		if (flageru == 1)
			break;
		MPI_Barrier(MPI_COMM_WORLD);
		if (rank != 0)
		{	//printf("envio 321\n");
			MPI_Ssend(&resultados, 1, rsp, 0, STD_TAG, MPI_COMM_WORLD);
		}
		else
		{
			for (int i = 1; i< n_proc; i++)
			{
				//printf("recv 327\n");
				MPI_Recv(&resultados, 1, rsp, i, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
				if (!resultados[0].found)
					fprintf(fout, "%s\nNOT FOUND\n", desc_query[i].line);
				for (int w = 0; w <= cabs; w++)
				{
					if(resultados[w].resposta > 0)
						fprintf(fout, "%s\n%d\n", marcadores[ resultados[w].indice ].text_cab, resultados[w].resposta); 
				}
			}
		}
	}

	// libera tudo os processos
	free(str);
	if (rank == 0)
		closefiles();
	
	MPI_Finalize();

	return EXIT_SUCCESS;
}