#include <iostream>
#include <atomic>
#include <unordered_map>
#include <algorithm>
#include <bitset>
#include "MapReduceFramework.h"
#include <pthread.h>
#include <semaphore.h>
#include "JobHandler.h"

#define ERROR_THREAD_CREATE "error creating thread"
#define ERROR_THREAD_JOIN "error joining thread"
#define SHIFT_BITS_STAGE 62
#define SHIFT_BITS_TWO 2
#define SHIFT_BITS_TOTAL 33
#define PERCENTAGE 100

void emit2 (K2 *key, V2 *value, void *context)
{
  IntermediatePair new_pair = IntermediatePair ({key, value});
  auto *thread_context = (IntermediateVec *) context;
  thread_context->push_back (new_pair);
}

void emit3 (K3 *key, V3 *value, void *context)
{
  auto *job = (JobHandler *) context;

  mutex_lock (&(job->mutex_emit));
  OutputPair out = OutputPair ({key, value});
  (job->output_vec).push_back (out);
  mutex_unlock (&(job->mutex_emit));
}

size_t find_thread_index (JobHandler *job)
{

  for (size_t i = 0; i < (size_t) job->n_of_thread; i++)
  {
    if (pthread_equal (pthread_self (), (job->threads)[i]))
    {
      return i;
    }
  }
  return -1;
}

void map_stage (JobHandler *job)
{
  size_t thread_index = find_thread_index (job);
  while (true)
  {
    uint64_t i = (*(job->atomic_counter))++;
    if (i >= ((job->input_vec).size ()))
    {
      job->map_barrier->barrier ();
      break;
    }
    job->client.map (((job->input_vec).at (i)).first,
                     ((job->input_vec).at (i)).second, (
                         (job->intermediate_vec) + thread_index));

    (*(job->atomic_state))++;
  }
  std::sort (((job->intermediate_vec) + thread_index)->begin (), (
                 (job->intermediate_vec)
                 + thread_index)->end (),
             [] (const IntermediatePair &lhs, const IntermediatePair &rhs)
             {
                 return (*(lhs.first) < *(rhs.first));
             });
}

void shuffle_stage (JobHandler *job)
{
  size_t num_of_pairs = 0;
  for (uint64_t i = 0; i < job->n_of_thread; i++)
  {
    num_of_pairs += (((job->intermediate_vec)[i]).size ());
  }
  job->updateState (MAP_STAGE, SHUFFLE_STAGE,
                    num_of_pairs);
  for (uint64_t i = 0; i < job->n_of_thread; i++)
  {
    {
      for (uint64_t j = 0; j < (((job->intermediate_vec)[i]).size ()); j++)
      {
        (job->intermediate_vec_shuffled).emplace
            ((((job->intermediate_vec)[i])[j]));
        (*(job->atomic_state))++;
      }
    }
  }
}

bool is_queue_empty (JobHandler *job)
{
  return (job->intermediate_vec_shuffled).empty ();
}

IntermediateVec process_batch (JobHandler *job, uint64_t *count)
{
  IntermediateVec new_vec;
  const K2 *top = (((job->intermediate_vec_shuffled).top ()).first);
  *count = 0;

  while (!(*(((job->intermediate_vec_shuffled).top ()).first) < *top))
  {
    new_vec.push_back ((job->intermediate_vec_shuffled).top ());
    (job->intermediate_vec_shuffled).pop ();
    (*count)++;
    if ((job->intermediate_vec_shuffled).empty ())
    {
      break;
    }
  }

  return new_vec;
}

void reduce_stage (JobHandler *job)
{
  while (true)
  {
    mutex_lock (&(job->mutex_reduce));

    if (is_queue_empty (job))
    {
      mutex_unlock (&(job->mutex_reduce));
      break;
    }
    uint64_t count = 0;
    IntermediateVec new_vec = process_batch (job, &count);

    mutex_unlock (&(job->mutex_reduce));

    (job->client).reduce (&new_vec, job);
    (*(job->atomic_state)) += count;
  }
}

void *thread_entry_point (void *jobHand)
{
  auto *job = (JobHandler *) jobHand;
  job->updateState (UNDEFINED_STAGE, MAP_STAGE,
                    (int) job->input_vec.size ());

  map_stage (job);
  job->barrier->barrier ();
  //shuffle
  if (pthread_equal (pthread_self (), (job->threads)[0]))
  {
    shuffle_stage (job);
  }
  job->barrier->barrier ();
  job->updateState (SHUFFLE_STAGE, REDUCE_STAGE,
                    job->intermediate_vec_shuffled.size ());
  reduce_stage (job);
  return nullptr;
}

JobHandle startMapReduceJob (const MapReduceClient &client,
                             const InputVec &inputVec, OutputVec &outputVec,
                             int multiThreadLevel)
{
  auto *job = new JobHandler (client, inputVec, outputVec, multiThreadLevel);
  for (int i = 0; i < multiThreadLevel; ++i)
  {
    if (pthread_create ((job->threads) + i,
                        nullptr, thread_entry_point, job)
        != 0)
      fail (ERROR_THREAD_CREATE);
  }
  return job;
}

void waitForJob (JobHandle job)
{
  auto *my_job = (JobHandler *) job;
  if (my_job->finished)
  {
    return;
  }
  mutex_lock (&(my_job->mutex_wait));
  my_job->finished = true;
  for (int i = 0; i < my_job->n_of_thread; i++)
  {
    if (pthread_join ((my_job->threads)[i],
                      nullptr) != SUCCESS)
      fail (ERROR_THREAD_JOIN);
  }
  mutex_unlock (&(my_job->mutex_wait));

}

void closeJobHandle (JobHandle job)
{
  waitForJob (job);
  auto *my_job = (JobHandler *) job;
  delete my_job;
}

void getJobState (JobHandle job, JobState *state)
{
  auto *j = (JobHandler *) job;
  uint64_t state_atomic = *(j->atomic_state);

  uint64_t stage_ = state_atomic;
  uint64_t total = state_atomic;
  uint64_t num_done = state_atomic;

  stage_ >>= SHIFT_BITS_STAGE;
  total <<= SHIFT_BITS_TWO;
  total >>= SHIFT_BITS_TOTAL;
  num_done <<= SHIFT_BITS_TOTAL;
  num_done >>= SHIFT_BITS_TOTAL;

  state->percentage = ((float) num_done) / ((float) total) * PERCENTAGE;
  state->stage = (stage_t) stage_;
}
