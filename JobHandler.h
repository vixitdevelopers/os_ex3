
#ifndef _JOBHANDLER_H_
#define _JOBHANDLER_H_

#include <csignal>
#include <atomic>
#include <queue>
#include <deque>
#include "MapReduceClient.h"
#include "Barrier.h"
#define SHIFT_STAGE 62
#define SHIFT_TOTAL 31
#define ERROR_LOCK_MUTEX "failed to lock mutex"
#define ERROR_UNLOCK_MUTEX "failed to unlock mutex"
#define ERROR_DESTROY_MUTEX "failed to destroy mutex"
#define SUCCESS 0

void fail (const char *msg)
{
  std::cout << msg << std::endl;
  exit (1);
}

class mycomparison
{
  bool reverse;
 public:
  mycomparison (const bool &revparam = false)
  { reverse = revparam; }
  bool
  operator() (const IntermediatePair &lhs, const IntermediatePair &rhs) const
  {
    if (reverse) return (*(rhs.first) < *(lhs.first));
    else return (*(lhs.first) < *(rhs.first));
  }
};



typedef std::deque<std::pair<K2 *, V2 *>> shuffle_pair;

typedef std::priority_queue<IntermediatePair, shuffle_pair, mycomparison>
    shuffle_type;

class JobHandler
{
 public:

  int n_of_thread;
  const MapReduceClient &client;
  const InputVec &input_vec;
  bool finished;
  OutputVec &output_vec;
  pthread_t *threads;
  std::atomic<uint64_t> *atomic_counter;
  std::atomic<uint64_t> *atomic_state;
  IntermediateVec *intermediate_vec;
  shuffle_type intermediate_vec_shuffled;
  pthread_mutex_t mutex_wait;
  pthread_mutex_t mutex_state;
  pthread_mutex_t mutex_reduce;
  pthread_mutex_t mutex_emit;
  Barrier *map_barrier;
  Barrier *barrier;

  JobHandler (const MapReduceClient &client,
              const InputVec &inputVec, OutputVec &outputVec,
              int multiThreadLevel)
      : client (client), input_vec (inputVec), output_vec
      (outputVec), n_of_thread (multiThreadLevel), mutex_wait (PTHREAD_MUTEX_INITIALIZER), mutex_state (PTHREAD_MUTEX_INITIALIZER),
        mutex_reduce (PTHREAD_MUTEX_INITIALIZER), mutex_emit (PTHREAD_MUTEX_INITIALIZER)
  {
    uint64_t init_value = (uint64_t) inputVec.size () <<
                                                      SHIFT_TOTAL |
                          (uint64_t) UNDEFINED_STAGE << SHIFT_STAGE;
    atomic_state = new std::atomic<uint64_t> (init_value);
    intermediate_vec = new IntermediateVec[multiThreadLevel];
    threads = new pthread_t[multiThreadLevel];
    finished = false;
    map_barrier = new Barrier (multiThreadLevel);
    barrier = new Barrier (multiThreadLevel);
    atomic_counter = new std::atomic<uint64_t> (0);

  }

  stage_t getJobStateFromAtomic () const
  {
    return static_cast<stage_t>(atomic_state->load () >> SHIFT_STAGE);
  }

  void updateState (stage_t stage_old, stage_t new_stage, size_t total_size)
  {
    if (pthread_mutex_lock (&mutex_state) != SUCCESS)
    {
      fail (ERROR_LOCK_MUTEX);
    }

    if (getJobStateFromAtomic () == stage_old)
    {
      uint64_t map_init_state = (uint64_t) total_size << SHIFT_TOTAL
                                | (uint64_t) new_stage << SHIFT_STAGE;
      *atomic_state = map_init_state;
    }

    if (pthread_mutex_unlock (&mutex_state) != SUCCESS)
    {
      fail (ERROR_UNLOCK_MUTEX);
    }
  }

  ~JobHandler ()
  {
    delete atomic_state;
    delete[] intermediate_vec;
    delete atomic_counter;
    delete[] threads;
    delete map_barrier;
    delete barrier;

    if (pthread_mutex_destroy (&mutex_state) != SUCCESS ||
        pthread_mutex_destroy (&mutex_reduce) != SUCCESS ||
        pthread_mutex_destroy (&mutex_wait) != SUCCESS
        || pthread_mutex_destroy (&mutex_emit)
           != SUCCESS)
    {
      fail (ERROR_DESTROY_MUTEX);
    }
  }
};

#endif //_JOBHANDLER_H_