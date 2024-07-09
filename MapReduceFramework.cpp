#include <iostream>
#include <atomic>
#include <unordered_map>
#include <algorithm>
#include <bitset>
#include "MapReduceFramework.h"
#include <pthread.h>
#include <semaphore.h>
#include "JobHandler.h"

void emit2 (K2 *key, V2 *value, void *context)
{
  IntermediatePair new_pair = IntermediatePair ({key, value});
  auto *thread_context = (IntermediateVec *) context;
  thread_context->push_back (new_pair);
}

void emit3 (K3 *key, V3 *value, void *context)
{
  JobHandler *job = (JobHandler *) context;

  if (pthread_mutex_lock (&(job->emit_mutex)) != 0)
  {
    fprintf (stdout, "system error: error\n");
    exit (1);
  }
  OutputPair out = OutputPair ({key, value});
  (job->outputVec).push_back (out);
  if (pthread_mutex_unlock (&(job->emit_mutex)) != 0)
  {
    fprintf (stdout, "system error: error\n");
    exit (1);
  }
}

void map_stage(JobHandler *job)
{
  while (true)
  {

    uint64_t i = (*(job->atomic_counter))++;
    if (i >= ((job->inputVec).size ()))
    {
      job->barrier->barrier ();
      break;
    }
    job->client.map (((job->inputVec).at (i)).first, ((job->inputVec).at (i)).second, (
        (job->emit2_pre) + i));
    //sort
    std::sort (((job->emit2_pre) + i)->begin (), ((job->emit2_pre)
                                                  + i)->end (),
               mysort);
    (*(job->atomic_state))++;
  }
}

void *foo (void *arg)
{
  auto *job = (JobHandler *) arg;
  job->updateState (UNDEFINED_STAGE, MAP_STAGE, (int) job->inputVec.size ());

  //map
  map_stage (job);
  //shuffle
  if (pthread_equal (pthread_self (), (job->threads)[0]))
  {
    auto c1 = (uint64_t) (job->inputVec.size ());
    uint64_t c2 = 2;
    c1 = c1 << 31;
    c2 = c2 << 62;
    uint64_t c = c1 | c2;
    (*(job->atomic_state)) = c;
    for (uint64_t i = 0; i < (job->inputVec).size (); i++)
    {
      {
        for (uint64_t j = 0; j < (((job->emit2_pre)[i]).size ()); j++)
        {
          (job->emit2_post).emplace ((((job->emit2_pre)[i])[j]));
        }
        (*(job->atomic_state))++;
      }
    }
    c1 = (uint64_t) (job->emit2_post.size ());
    c2 = 3;
    c1 = c1 << 31;
    c2 = c2 << 62;
    c = c1 | c2;
    (*(job->atomic_state)) = c;
  }
  job->barrier2->barrier ();
  //reduce
  while (true)
  {
    if (pthread_mutex_lock (&(job->mutex)) != 0)
    {
      fprintf (stdout, "system error: error\n");
      exit (1);
    }
    if ((job->emit2_post).empty ())
    {
      if (pthread_mutex_unlock (&(job->mutex)) != 0)
      {
        fprintf (stdout, "system error: error\n");
        exit (1);
      }
      break;
    }
    IntermediateVec new_vec = IntermediateVec ();
    const K2 *top = (((job->emit2_post).top ()).first);
    uint64_t c = 0;
    while (!(*(((job->emit2_post).top ()).first) < *top))
    {
      new_vec.push_back ((job->emit2_post).top ());
      (job->emit2_post).pop ();
      c++;
      if ((job->emit2_post).empty ())
      {
        break;
      }
    }
    if (pthread_mutex_unlock (&(job->mutex)) != 0)
    {
      fprintf (stdout, "system error: error\n");
      exit (1);
    }
    (job->client).reduce (&new_vec, job);
    (*(job->atomic_state)) += c;
  }
  return nullptr;
}

JobHandle startMapReduceJob (const MapReduceClient &client,
                             const InputVec &inputVec, OutputVec &outputVec,
                             int multiThreadLevel)
{
  JobHandler *job = new JobHandler (client, inputVec, outputVec, multiThreadLevel);
  for (int i = 0; i < multiThreadLevel; ++i)
  {
    if (pthread_create ((job->threads) + i, NULL, foo, job) != 0)
    {
      fprintf (stdout, "system error: error\n");
      exit (1);
    }
  }
  return job;
}

void waitForJob (JobHandle job)
{
  JobHandler *j = (JobHandler *) job;
  if (j->done)
  {
    return;
  }
  if (pthread_mutex_lock (&(j->wait_mutex)) != 0)
  {
    fprintf (stdout, "system error: error\n");
    exit (1);
  }
  j->done = true;
  for (int i = 0; i < j->multiThreadLevel; i++)
  {
    if (pthread_join ((j->threads)[i], NULL) != 0)
    {
      fprintf (stdout, "system error: error\n");
      exit (1);
    }
  }
  if (pthread_mutex_unlock (&(j->wait_mutex)) != 0)
  {
    fprintf (stdout, "system error: error\n");
    exit (1);
  }

}
void getJobState (JobHandle job, JobState *state)
{
  JobHandler *j = (JobHandler *) job;
  uint64_t sdc = *(j->atomic_state);
  uint64_t s = sdc;
  uint64_t d = sdc;
  uint64_t c = sdc;
  s = s >> 62;
  d = d << 2;
  d = d >> 33;
  c = c << 33;
  c = c >> 33;
  state->percentage = ((float) c) / ((float) d) * 100;
  state->stage = (stage_t) s;
}
void closeJobHandle (JobHandle job)
{
  waitForJob (job);
  JobHandler *j = (JobHandler *) job;
  delete j;
}
