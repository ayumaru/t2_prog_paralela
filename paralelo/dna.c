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
} marcador;

typedef struct{
	char line[100]; 
} cab_q;

typedef struct{
	int indice;
	int resposta;
	int found;
} acertos;

marcador marcadores[30];



int preprocessamento(){
	char linha[100];
	int j;
	int cabs = 1;
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
		cabs++;
		j++;
		strcpy( marcadores[j].text_cab, linha );
	}
	cabs = cabs-1;

	return cabs;	
}



char *str;
char *str0;

//msg, tamanho da msg, tipo do dado, comunicador que ta enviando, comunicador que vai receber
void envia_reconstroi(int cabs, int rank, MPI_Status status, MPI_Datatype pixtype){ 

	for(int y=0; y <= cabs; y=y+1)
		MPI_Bcast(&marcadores[y], 1, pixtype, 0, MPI_COMM_WORLD); 

}
	


int main(int argc, char** argv) {
	
	int cabs;
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
	str0 = (char*) malloc(sizeof(char) * 1000001);
	if (str0 == NULL) {
		perror("malloc str0\n");
		exit(EXIT_FAILURE);
	}
	
	
	int count = 2;
	int lengths[2] = {100,1000001};
	MPI_Datatype oldtypes[2] = {MPI_CHAR,MPI_CHAR};
	MPI_Aint offsets[2] = {0,100};

	
	MPI_Type_struct(count,lengths,offsets,oldtypes,&pixtype);
	MPI_Type_commit(&pixtype);


	count = 3;
	int tams[3] = {1,1,1};
	MPI_Datatype n_oldtypes[3] = {MPI_INT, MPI_INT, MPI_INT};
	MPI_Aint n_offsets[3] = {0,sizeof(int),sizeof(int)+sizeof(int)};
	MPI_Type_struct(count, tams, n_offsets, n_oldtypes, &rsp);
	MPI_Type_commit(&rsp);


	cabs = 0;
	// so se for o processo 0
	if (rank == 0)
	{
		openfiles();
		cabs = preprocessamento();

		for(int i=1; i<n_proc && n_proc > 1; i++)
			MPI_Ssend(&cabs, 1, MPI_INT, i, STD_TAG, MPI_COMM_WORLD);
	}
	
	if (rank != 0) // || n_proc == 1)
		MPI_Recv(&cabs, 1, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
	
	

	MPI_Barrier(MPI_COMM_WORLD);
	cab_q desc_query[n_proc+1];	
	acertos resultados[cabs];

	if(n_proc > 1)
		envia_reconstroi(cabs, rank, status, pixtype);

	MPI_Barrier(MPI_COMM_WORLD);

	if(rank == 0)
	{
		if(feof(fquery)) // se fim = true
			flageru = 1;

		for(int i=1; i<n_proc && n_proc > 1; i++)
			MPI_Ssend(&flageru, 1, MPI_INT, i, STD_TAG, MPI_COMM_WORLD);
	}

	if (rank != 0)// || n_proc == 1)
		MPI_Recv(&flageru, 1, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

	if (rank == 0)
	{
		fgets(line, 100, fquery); //rever a posicao de escrita disso
		remove_eol(line);
	}
	
	while (flageru == 0){
		
		str[0] = 0;
		str0[0] = 0;
		MPI_Barrier(MPI_COMM_WORLD);
		if (rank == 0)
		{
			if(feof(fquery)) // se fim = true
			{
				flageru = 1;
			}
			
			for(int i=1; i<n_proc && n_proc > 1; i++)
				MPI_Ssend(&flageru, 1, MPI_INT, i, STD_TAG, MPI_COMM_WORLD);
			
			if (flageru == 0)
			{ 
				
				for (int q = 0; q < n_proc; q++ )
				{
					strcpy(desc_query[q].line, line); 

						// read query string
					fgets(line, 100, fquery);  
					remove_eol(line);
					i = 0;
					if(n_proc > 1 && q > 0)
						str[0] = 0;
					do { 
						strcat(str, line); 
						if (fgets(line, 100, fquery) == NULL)
							break;
						remove_eol(line);
					} while (line[0] != '>');

					if(q == 0)
						strcpy(str0,str);

					if(n_proc > 1 && q > 0)
					{
						// printf("oi, entrei pra entregar mensagem\n");
						// printf("meu rank: %d, minha query: %.20s \n", q, str );
						MPI_Ssend(str, strlen(str)+1, MPI_CHAR, q, STD_TAG, MPI_COMM_WORLD);
						str[0] = 0;
					}
				}
			}
		}
		
		resultados[0].found = 0;
		
		if(n_proc > 1 && rank != 0)
		{	//printf("recebi a mensagem do flageru 278 \n");
			MPI_Recv(&flageru, 1, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
		}
		if (flageru == 0) 
		{
			if (n_proc > 1 && rank !=0)
			{	//printf("recebendo minha query 284 \n");
				MPI_Recv(str, 1000001, MPI_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
				resultados[0].found = 0;
			}

			if(rank != 0)
			{
				for(int y=0; y < cabs; y=y+1)
				{	
					resultados[y].resposta = bmhs( marcadores[y].genoma, strlen(marcadores[y].genoma), str, strlen(str)); 
					resultados[y].indice = y;
					// printf("rank: %d || indice: %d || resposta: %d || query: %.20s \n", rank, y, resultados[y].resposta, desc_query[rank].line);

					if (resultados[y].resposta > 0)
						resultados[0].found++;					
				}
			}
			else
			{
				for(int y=0; y < cabs; y=y+1)
				{	
					resultados[y].resposta = bmhs( marcadores[y].genoma, strlen(marcadores[y].genoma), str0, strlen(str0)); 
					resultados[y].indice = y;
					// printf("rank: %d || indice: %d || resposta: %d || query: %.20s \n", rank, y, resultados[y].resposta, desc_query[rank].line);

					if (resultados[y].resposta > 0)
						resultados[0].found++;					
				}
			}


			if (rank == 0) //sempre vai entrar, agora que o 0 eh obrigado a trabalhar
			{
				fprintf(fout, "%s\n", desc_query[0].line);
				
				for (int w = 0; w < cabs; w++)
				{
					if(resultados[w].resposta > 0)
						fprintf(fout, "%s\n%d\n", marcadores[ resultados[w].indice ].text_cab, resultados[w].resposta); 
				}
				if (!resultados[0].found)
					fprintf(fout, "NOT FOUND\n");
			}

		}
		
		// segfault ta aqui pra baixo
		if (flageru == 1)
			break;
		

		MPI_Barrier(MPI_COMM_WORLD);
		
		
		
		if (rank != 0)
			MPI_Ssend(&resultados, cabs, rsp, 0, STD_TAG, MPI_COMM_WORLD);
		else if (rank == 0 && n_proc > 1)
		{
			
			//printf("entrei aqui na 322 \n");
			for (int i = 1; i < n_proc; i++) // se for so 1 processo nem entra aqui
			{

				if(desc_query[i].line[0] != '>')
				{
					acertos tmp_d[cabs];
					MPI_Recv(&tmp_d, cabs, rsp, i, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
				}
				else
				{
					fprintf(fout, "%s\n", desc_query[i].line);
					MPI_Recv(&resultados, cabs, rsp, i, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
					if (!resultados[0].found)
							fprintf(fout, "NOT FOUND\n");

					for (int w = 0; w < cabs; w++)
					{
						if(resultados[w].resposta > 0)
							fprintf(fout, "%s\n%d\n", marcadores[ resultados[w].indice ].text_cab, resultados[w].resposta); 
					}
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