//
// Created by dani on 7/9/24.
//

#ifndef _JOBHANDLER_H_
#define _JOBHANDLER_H_

#include <csignal>
#include <atomic>
#include <queue>
#include <deque>
#include "MapReduceClient.h"
#include "Barrier.h"
#define STAGE_BITS_SHIFT 62
#define TOTAL_KEYS_BITS_SHIFT 31
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

struct mysort
{
    bool
    operator() (const IntermediatePair &lhs, const IntermediatePair &rhs) const
    {
      return (*(lhs.first) < *(rhs.first));
    }
} mysort;
typedef std::deque<std::pair<K2 *, V2 *>> shuffle_pair;

typedef std::priority_queue<IntermediatePair, shuffle_pair, mycomparison>
    shuffle_type;

class JobHandler
{
 public:

  Barrier *barrier;
  Barrier *barrier2;
  const MapReduceClient &client;
  const InputVec &input_vec;
  OutputVec &output_vec;
  pthread_t *threads;
  std::atomic<uint64_t> *atomic_counter;
  std::atomic<uint64_t> *atomic_state;
  int n_of_thread;
  IntermediateVec *emit2_pre;
  shuffle_type emit2_post;
  pthread_mutex_t mutex_wait;
  pthread_mutex_t mutex_state;
  pthread_mutex_t mutex_reduce;
  pthread_mutex_t mutex_emit;

  int num_of_threads;
  bool done;
  JobHandler (const MapReduceClient &client,
              const InputVec &inputVec, OutputVec &outputVec,
              int multiThreadLevel)
      : client (client), input_vec (inputVec), output_vec
      (outputVec), mutex_reduce (PTHREAD_MUTEX_INITIALIZER), mutex_emit (PTHREAD_MUTEX_INITIALIZER), mutex_wait (PTHREAD_MUTEX_INITIALIZER),
        mutex_state (PTHREAD_MUTEX_INITIALIZER), n_of_thread (multiThreadLevel)
  {
    threads = new pthread_t[multiThreadLevel];
    emit2_pre = new IntermediateVec[multiThreadLevel];
    atomic_counter = new std::atomic<uint64_t> (0);
    num_of_threads = multiThreadLevel;
    uint64_t init_value = (uint64_t) inputVec.size () <<
                                                      TOTAL_KEYS_BITS_SHIFT |
                          (uint64_t) UNDEFINED_STAGE << STAGE_BITS_SHIFT;
    atomic_state = new std::atomic<uint64_t> (init_value);
    barrier = new Barrier (multiThreadLevel);
    barrier2 = new Barrier (multiThreadLevel);
    done = false;
  }

  stage_t getJobStateFromAtomic () const
  {
    return static_cast<stage_t>(atomic_state->load () >> STAGE_BITS_SHIFT);
  }

  void updateState (stage_t prev_stage, stage_t new_stage, size_t total)
  {
    if (pthread_mutex_lock (&mutex_state) != 0)
    {
      printf ("failed to lock a update_stage_mutex");
    }

    if (getJobStateFromAtomic () == prev_stage)
    {
      uint64_t map_init_state = (uint64_t) total << TOTAL_KEYS_BITS_SHIFT
                                | (uint64_t) new_stage << STAGE_BITS_SHIFT;
      *atomic_state = map_init_state;
    }

    if (pthread_mutex_unlock (&mutex_state) != 0)
    {
      printf ("failed to unlock a update_stage_mutex");
    }
  }

  ~JobHandler ()
  {
    delete[] threads;
    delete[] emit2_pre;
    delete atomic_state;
    delete barrier;
    delete barrier2;
    delete atomic_counter;
    if (pthread_mutex_destroy (&mutex_reduce) != 0
        || pthread_mutex_destroy (&mutex_emit)
           != 0 || pthread_mutex_destroy (&mutex_wait) != 0
        || pthread_mutex_destroy (&mutex_state) != 0)
    {
      fprintf (stdout, "system error: error\n");
      exit (1);
    }
  }
};

#endif //_JOBHANDLER_H_
