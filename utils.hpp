// openMP utility functions
// Taken from leader-adv lab - John Gash
#ifdef _OPENMP
#include "omp.h"
#endif

int myThreadNum()
{
#ifdef _OPENMP
   return omp_get_thread_num();
#else
   return 0;
#endif
}

int numThreads()
{
#ifdef _OPENMP
   return omp_get_max_threads();
#else
   return 1;
#endif
}
