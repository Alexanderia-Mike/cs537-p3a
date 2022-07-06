this is a course project for the course cs 547, Into. to Operating Systems in University of Wisconsin, Madison.

### Brief Introduction

In this project I implemented a Map-Reduce algorithm that can be applied to many different applications, such as word counting (which is the example shown in [main.c](https://github.com/Alexanderia-Mike/cs537-p3a/blob/main/main.c). 

The Map-Reduce algorithm consists of two process. In the first process a number of mappers concurrently map the input data to some intermediate key-value pairs, and in the second process a number of reducers group together the intermediate key-value pairs and calculate the final answer

In both mapping step and reduce step, the algorithm supports high concurrency to speed up the calculation. Also, mutex read-write locks are implemented in several global variables, including a hash-map and a list for intermediate key-value pairs.

The detailed requirements of the project can be found in []().
