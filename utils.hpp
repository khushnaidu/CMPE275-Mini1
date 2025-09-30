/**
 * simple functions to help with openMP
*/

// Conditional include: only use OpenMP if available
#ifdef _OPENMP
#include "omp.h"
#endif

/**
 * protect against compiling without -fopenmp
 * Returns the ID of the current thread (0 if not using OpenMP)
 */
int myThreadNum()
{
#ifdef _OPENMP
   // omp_get_thread_num() returns 0 for main thread, 1+ for worker threads
   return omp_get_thread_num();
#else
   return 0;
#endif
}

/**
 * protect against compiling without -fopenmp
 * Returns the maximum number of threads available
 */
int numThreads()
{
#ifdef _OPENMP
   // Returns number of threads OpenMP will use in parallel regions
   return omp_get_max_threads();
#else
   return 1;
#endif
}
