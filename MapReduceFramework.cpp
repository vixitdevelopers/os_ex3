#include <iostream>
#include <atomic>
#include <unordered_map>
#include <algorithm>
#include <bitset>
#include "MapReduceFramework.h"
#include <pthread.h>
#include <semaphore.h>
#include "JobHandler.h"


void emit2 (K2* key, V2* value, void* context){
  IntermediatePair new_pair = IntermediatePair ({key,value});
  auto * thread_context = (IntermediateVec * ) context;
  thread_context->push_back(new_pair);
}

void emit3 (K3* key, V3* value, void* context){
  JobHandler* job = (JobHandler*) context;

  if (pthread_mutex_lock(&(job->emit_mutex)) != 0){
    fprintf(stdout, "system error: error\n");
    exit(1);
  }
  OutputPair out = OutputPair ({key,value});
  (job->output_vec).push_back(out);
  if (pthread_mutex_unlock (&(job->emit_mutex)) != 0)
  {
    fprintf (stdout, "system error: error\n");
    exit (1);
  }
}

size_t find_thread_index(JobHandler * job){

  for (size_t i = 0; i < (size_t) job->n_of_thread; i++)
  {
    if (pthread_equal (pthread_self (), (job->threads)[i]))
    {
      return i;
    }
  }
  return -1;
}

void map_stage(JobHandler *job)
{
  size_t thread_index = find_thread_index(job);
  if (thread_index == -1)
  {
    fprintf (stdout, "system error: error\n");
    exit (1);
  }
  while (true)
  {

    uint64_t i = (*(job->atomic_counter))++;
    if (i >= ((job->input_vec).size ()))
    {
      job->barrier->barrier ();
      break;
    }
    job->client.map (((job->input_vec).at (i)).first, ((job->input_vec).at (i)).second, (
        (job->emit2_pre) + thread_index));

    (*(job->atomic_state))++;
  }
  //sort
  std::sort (((job->emit2_pre) + thread_index)->begin (), ((job->emit2_pre)
                                                           + thread_index)->end (),
             mysort);
}

void shuffle_stage(JobHandler *job)
{
    size_t num_of_pairs = 0;
    for (uint64_t i = 0; i < job->num_of_threads; i++){
        num_of_pairs += (((job->emit2_pre)[i]).size ());
    }
    job->updateState(MAP_STAGE, SHUFFLE_STAGE,
                   num_of_pairs);
    for (uint64_t i = 0; i < job->num_of_threads; i++)
    {
      {
        for (uint64_t j = 0; j < (((job->emit2_pre)[i]).size ()); j++)
        {
          (job->emit2_post).emplace ((((job->emit2_pre)[i])[j]));
          (*(job->atomic_state))++;
        }
      }
    }
}

void lock_mutex(pthread_mutex_t *mutex) {
  if (pthread_mutex_lock(mutex) != 0) {
    fprintf(stdout, "system error: error\n");
    exit(1);
  }
}

void unlock_mutex(pthread_mutex_t *mutex) {
  if (pthread_mutex_unlock(mutex) != 0) {
    fprintf(stdout, "system error: error\n");
    exit(1);
  }
}

bool is_queue_empty(JobHandler *job) {
  return (job->emit2_post).empty();
}

IntermediateVec process_batch(JobHandler *job, uint64_t *count) {
  IntermediateVec new_vec;
  const K2 *top = (((job->emit2_post).top()).first);
  *count = 0;

  while (!(*(((job->emit2_post).top()).first) < *top)) {
    new_vec.push_back((job->emit2_post).top());
    (job->emit2_post).pop();
    (*count)++;
    if ((job->emit2_post).empty()) {
      break;
    }
  }

  return new_vec;
}

void reduce_stage(JobHandler *job) {
  while (true) {
    lock_mutex(&(job->mutex));

    if (is_queue_empty(job)) {
      unlock_mutex(&(job->mutex));
      break;
    }

    uint64_t count = 0;
    IntermediateVec new_vec = process_batch(job, &count);

    unlock_mutex(&(job->mutex));

    (job->client).reduce(&new_vec, job);
    (*(job->atomic_state)) += count;
  }
}





void* foo(void* arg)
{
  auto* job = (JobHandler*) arg;
  job->updateState(UNDEFINED_STAGE, MAP_STAGE, (int) job->input_vec.size());

  map_stage(job);
  //shuffle
  if(pthread_equal ( pthread_self(), (job->threads)[0])){

    shuffle_stage (job);
  }
  job->barrier2->barrier();
  job->updateState(SHUFFLE_STAGE, REDUCE_STAGE,
                   job->emit2_post.size());
  //reduce
  reduce_stage(job);
  return nullptr;
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
  JobHandler* job = new JobHandler(client, inputVec, outputVec, multiThreadLevel);
  for (int i = 0; i < multiThreadLevel; ++i) {
    if(pthread_create((job->threads) + i, NULL, foo, job)!=0){
      fprintf (stdout, "system error: error\n");
      exit (1);
    }
  }
  return job;
}




void waitForJob(JobHandle job){
  JobHandler* j = (JobHandler*)job;
  if(j->done){
    return;
  }
  if (pthread_mutex_lock(&(j->wait_mutex)) != 0){
    fprintf(stdout, "system error: error\n");
    exit(1);
  }
  j->done= true;
  for(int i = 0; i<j->n_of_thread; i++){
    if(pthread_join((j->threads)[i], NULL)!=0){
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
void getJobState(JobHandle job, JobState* state){
  JobHandler* j = (JobHandler*)job;
  uint64_t sdc = *(j->atomic_state);
  uint64_t s = sdc;
  uint64_t d = sdc;
  uint64_t c = sdc;
  s=s>>62;
  d=d<<2;
  d=d>>33;
  c=c<<33;
  c=c>>33;
  state->percentage = ((float)c)/((float)d)*100;
  state->stage = (stage_t)s;
}

void closeJobHandle(JobHandle job){
  waitForJob(job);
  JobHandler *j = (JobHandler*)job;
  delete j;
}
