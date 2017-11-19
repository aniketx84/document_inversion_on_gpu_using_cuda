# Document Inversion Using Cuda
##Introduction
This repository contains program to accelerate the creation of inverted Index by leveraging the power of GPU to parallelise the index creation process. It is based on Nvidia CUDA.
It uses two ways parallelism. One block is used to processed one document so large number of documents can be processed in parallel. Within each blocks the execution is also parallelises as threads are used to process each element of the document.
This is loosly based on the map-reduce framework.The project needs saveral improvement in memory management,optimising performance.
