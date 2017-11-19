#include <sys/types.h>
#include <dirent.h>
#include <iostream>
#include <cuda.h>
#include <cstdlib>
#include <vector>
#include <algorithm>
#include <cstdio>
#include <ctime>
#include <fstream>
#include <string>
#include <cerrno>

#include <cub/cub.cuh>

#include "gputimer.h"
#include "stopwords.h"


using namespace std;



struct myVec
{
	char a[4096];
	int b;
};

struct posting
{
    unsigned long long key;
    int value;
};


__shared__ char sh_document[4097];
//__shared__ char sh_check[4097];
//__shared__ long sh_hash_val[4097];
__shared__ unsigned long long sh_tokens[4097];



__device__ int len(int i)
{
	int len = 0;
	for(;sh_tokens[i]==0;i++,len++);
	return len;
}


__device__ __host__ unsigned long SDBM(const char* str, unsigned int length)
{
	unsigned long hash = 0;
	unsigned int i;
	for(i = 0; i < length; ++str, i++)
	{
		hash = (*str) + (hash << 6) + (hash << 16) - hash;
	}
	return hash;
}

__device__ bool isAlpha(char ch)
{
	if(((ch>='a')&&(ch<='z'))||((ch>='A')&&(ch<='Z')))
		return true;
	else
    	return false;
}



__global__ void map(myVec *text,posting *d_out,unsigned long long int  *tell)
{ 
   
	//copying the document to shared memory
	int16_t t = threadIdx.x;
	int i=0;
	while((t<text[blockIdx.x].b)&&(t<4096))
	{
		sh_document[t] = text[blockIdx.x].a[t];
		//sh_check[t] = 0;
		sh_tokens[t] = 0;
		//sh_hash_val[t] = 0;
		i++;
		t = i*blockDim.x+threadIdx.x;
	}
	__syncthreads();
	sh_document[4096] = 0;
    //sh_check[4096] = 0;
	sh_tokens[4096] = 0;



    //Tokenising the document
    t = threadIdx.x;
    i = 0;
    while((t<text[blockIdx.x].b)&&(t<4097)&&(t!=0))
    {
    	if((isAlpha(sh_document[t]))&&(t!=0))
    	{
    		/*if((sh_document[t]>=65)&&(sh_document[t]<=90))
    			sh_document[i] = sh_document[i] + 32;*/
    		if((!isAlpha(sh_document[t-1]))&&(sh_tokens[t-1]==0))
    		{
    			atomicAdd(&sh_tokens[t-1],t-1);
    			
    		}
    		if(!isAlpha(sh_document[t+1])&&(t<4095)&&(sh_tokens[t+1]==0))
    		{
    			atomicAdd(&sh_tokens[t+1],t+1);
    			
    		}
    	}
    	else if(t!=0)
    	{
    		if(!isAlpha(sh_document[t-1])&&!isAlpha(sh_document[t+1])&&(sh_tokens[t]==0))
    		{
    			atomicAdd(&sh_tokens[t],t);
    		}
    	}
    	i++;
    	t = i*blockDim.x+threadIdx.x;
    }
    __syncthreads();

  
    // Creating the hash
    t = threadIdx.x;
    i = 0;
    while((t<text[blockIdx.x].b)&&(t<4097)&&(t!=0))
    {
    	
        if((sh_tokens[t]==0)&&(sh_tokens[t-1]!=0))
        	sh_tokens[t] = SDBM(&sh_document[t],len(t));
        else
        	sh_tokens[t] = 0;
    	i++;
    	t = i*blockDim.x+threadIdx.x;
    }
    __syncthreads();

    
    //Removing the stopwords
    t = threadIdx.x;
    i = 0;
    while((t<4097))
    {
        if(sh_tokens[i]!=0)
        	for(int j=0;j<174;j++)
        	{
        		/*if(sh_tokens[i]==d_stopwords[j])
        			atomicSub(&sh_tokens[i],sh_tokens[i]);//*/
        		atomicCAS(&sh_tokens[i],d_stopwords[j],0);
        	}
    	i++;
    	t = i*blockDim.x+threadIdx.x;
    }
    __syncthreads();



    /*//sorting algorithm
    if(threadIdx.x==0)
    {
    	for(int i = 0; i < 4097; i++)
    	{
    		for(int j = 0; j < 4097-i-1; j++)
    		{
    			if(sh_tokens[j]>sh_tokens[j+1])
    			{
    				unsigned long long temp = sh_tokens[j];
    				sh_tokens[j] = sh_tokens[j+1];
    				sh_tokens[j+1] = temp;
    			}
    		}
    	}
    }//*/

    //creating the postings
    if(blockIdx.x<588){
    t = threadIdx.x;
    i = 0;
    while(t<4097)
    {
        d_out[blockIdx.x*4097+t].key = sh_tokens[t];
        d_out[blockIdx.x*4097+t].value = blockIdx.x;
    	i++;
    	t = i*blockDim.x+threadIdx.x;
    }
    __syncthreads();
    }//*/


    /*int count = 0;
	unsigned long long t = ptr[threadIdx.x];
	__syncthreads();

	for(int i = 0; i < len; i++){
		if(ptr[i]<t)
			count++;
	}
	__syncthreads();
	ptr[count] = t;
    */



    
   /*   -----------------------needed later---------------------------  
   t = threadIdx.x;
    i=0;
    while((t<text[blockIdx.x].b)&&(t<4097))
	{
		if((sh_tokens[t]==0)&&(sh_document[t] != '\n')){
		sh_check[t] = sh_document[t];}
		else if(sh_tokens[t-1]==0)
			sh_check[t] = '\0';
		else
			sh_check[t] = '\0';
		i++;
		t = i*blockDim.x+threadIdx.x;
	}
	__syncthreads();
	------------------------------------------------------------------------*/

    
   ///*--------------------------------------------for checking values in shared memory------------------------------
	if((threadIdx.x==0)&&(blockIdx.x==0))
	{
		for(int i = 0; i < 4097;i++)
			tell[i] = sh_tokens[i];

	}
    //-------------------------------------------------------------------------------------------------------------*/
}

void read_directory(const string& name, vector<string>& v)
{
    DIR* dirp = opendir(name.c_str());
    struct dirent * dp;
    while ((dp = readdir(dirp)) != NULL) {
    	if(dp->d_name[0]=='.')
    		continue;
        v.push_back(dp->d_name);
    }
    closedir(dirp);
}


string get_file_content(const char* filename)
{
	ifstream in(filename, std::ios::in | std::ios::binary);
	if (in)
	{
		std::string contents;
		in.seekg(0, std::ios::end);
		contents.resize(in.tellg());
		in.seekg(0, std::ios::beg);
		in.read(&contents[0], contents.size());
		in.close();
		return(contents);
	}
	throw(errno);
}

int main(int argc,char** argv)
{
	unsigned long long int count;
	string path = "/home/aniket/out";
	char ch='y';
	unsigned long long int  *tell;
	int fileNumber; 
	posting *d_out;
	myVec *d_fileContent;
	vector<string> fileList;
	vector<string> fileContent;
	int *d_docId;
	cudaError_t ce;
	//cout<<"Enter the path to dataset:\n";
	//getline(cin,path);


	read_directory(path,fileList);

	//-----------------------------------------------to be use to debug problems with file lsiting-----------------------------
	//	for(vector<string>::iterator it=fileList.begin();it!=fileList.end();++it)
	//	{
	//		cout<<*it<<endl;
	//	}
	//-------------------------------------------------------------------------------------------------------------------------

     for(unsigned int i=0;i<fileList.size();i++)
		fileContent.push_back(get_file_content((path+"/"+fileList[i]).c_str()));  //skipped first two files


    

	cout << "No of files copied "<<fileContent.size()<<endl;


    cudaMallocManaged((void**)&d_fileContent,(fileContent.size())*sizeof(myVec));
    //cudaMallocManaged((void**)&t,24576*sizeof(int));
    cudaMallocManaged((void**)&tell,4097*sizeof(unsigned long long int));
    ce = cudaMallocManaged((void**)&d_out, 4096*fileList.size()*sizeof(posting));

    cout<<cudaGetErrorString(ce)<<endl;

    
    for(int i=0;i<fileContent.size();i++)
    {
    	//d_fileContent[i].a = (char*)malloc(fileContent[i].size()*sizeof(char));
    	cudaMallocManaged((void**)&tell[i],4096*sizeof(char));
    	d_fileContent[i].b = fileContent[i].size();
    	strcpy(d_fileContent[i].a,fileContent[i].c_str());
    }

    //cudaMemcpyToSymbol(d_stopwords, stopword, 174*sizeof(unsigned long long));;

     GpuTimer kernelTimer,sortTimer;
     kernelTimer.Start();
     map<<<fileContent.size(),128>>>(d_fileContent,d_out,tell);
     kernelTimer.Stop();
     ce = cudaDeviceSynchronize();
     cout<<cudaGetErrorString(ce)<<endl;


     
     //--------------------------------------------------------For sorting-----------------------------------------------------
    unsigned long long *d_keys_in;      //array to hold the hashed terms
    int *d_values_in;                   //array to hold the docId
    unsigned long long *d_keys_out;     //array to hold the sorted hashed terms
    int *d_values_out;                  //array to hold the corrosponding docId
    int num_items = 4096*fileList.size();           //Number of element to be sorted

    void *d_temp_storage = NULL;        //auxallary space required for the sorting algorithm
    size_t temp_storage_bytes = 0;      //Size of the auxallary storage


    //Alocating spaces for the above variables
    cudaMallocManaged((void**)&d_values_out, num_items*sizeof(int));
    cudaMallocManaged((void**)&d_keys_out, num_items*sizeof(unsigned long long));
    cudaMallocManaged((void**)&d_values_in, num_items*sizeof(int));
    cudaMallocManaged((void**)&d_keys_in, num_items*sizeof(unsigned long long));

    //copying the keys and values form the output of mapper to the input of array for sorting
    for(unsigned long long i = 0; i < num_items; i++)
    {
        d_keys_in[i] = d_out[i].key;
        d_values_in[i] = d_out[i].value;
    }
     
    cub::DeviceRadixSort::SortPairs(d_temp_storage, temp_storage_bytes,
    d_keys_in, d_keys_out, d_values_in, d_values_out, num_items);

    //Alocating auxallary space
    cudaMalloc(&d_temp_storage, temp_storage_bytes);
    sortTimer.Start();
    cub::DeviceRadixSort::SortPairs(d_temp_storage, temp_storage_bytes,
    d_keys_in, d_keys_out, d_values_in, d_values_out, num_items);
    cudaDeviceSynchronize();//*/
    sortTimer.Stop();

    //------------------------------------------------------------------------------------------------------------------------




     if(ce==cudaSuccess)
     	cout<<"Index Created Sucessfully...!!"<<endl;
     else
     {	
     	cout<<"Index creation failed: "<<endl;
     	exit(1);
     }

    
    
    /*-------------------------------------------------------to be used to check file content-----------------------------------
		while (ch == 'y')
		{
			cout << "Enter the file number to be printed: " << endl;
			cin >> fileNumber;
			if (fileNumber < fileContent.size())
			cout << d_fileContent[fileNumber].a;
			cout << "Continue..?(y/n): ";
			cin >> ch;
		}
    //---------------------------------------------------------------------------------------------------------------------------*/
	  
    //---------------------------------------------------------Printing the execution time----------------------------------------

        cout<<"\nKernel Execution time: "<<kernelTimer.Elapsed()<<" ms"<<endl;
        cout<<"\nSorting time:          "<<sortTimer.Elapsed()<<" ms"<<endl;
		cout<<"\n";

    //----------------------------------------------------------------------------------------------------------------------------







    
    //--------------------------------------------------performing the search operation-------------------------------------------

        string term;
        GpuTimer searchTimer;
        cout<<"Enter the search term: ";
        cin>>term;
        //std::transform(term.begin(),term.end(),term.begin(),::tolower);
        unsigned long long term_hash = SDBM(term.c_str(),term.size());
        searchTimer.Start();
        for(unsigned long long i = num_items/2; i < num_items; i++)
        {
            if(d_keys_out[i]==term_hash)
                cout<<fileList[d_values_out[i]]<<endl;
        }
        searchTimer.Stop();


    //----------------------------------------------------------------------------------------------------------------------------


	/*for(int i=0;i<4096;i++)
	{
		cout<<d_out[4096*550+i].key<<" ";
	}//*/

    cout<<"\nSearch time: "<<searchTimer.Elapsed()<<" ms"<<endl;

	//cout<<fileContent[0];
	cout<<"\n";
	cudaFree(d_out);
	cudaFree(d_fileContent);
    cudaFree(d_values_in);
    cudaFree(d_keys_in);
    cudaFree(d_values_out);
    cudaFree(d_keys_out);
	return 0;
}
