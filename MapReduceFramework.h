#ifndef MAPREDUCEFRAMEWORK_H
#define MAPREDUCEFRAMEWORK_H

#include "MapReduceClient.h"

typedef void* JobHandle;

enum stage_t {UNDEFINED_STAGE=0, MAP_STAGE=1, SHUFFLE_STAGE=2, REDUCE_STAGE=3};

typedef struct {
	stage_t stage;
	float percentage;
} JobState;

void emit2 (K2* key, V2* value, void* context);
void emit3 (K3* key, V3* value, void* context);

JobHandle start_map_reduce_job(const MapReduceClient& client,
                               const InputVec& inputVec, OutputVec& outputVec,
                               int multiThreadLevel);

void wait_for_job(JobHandle job);
void get_job_state(JobHandle job, JobState* state);
void close_job_handle(JobHandle job);
	
	
#endif //MAPREDUCEFRAMEWORK_H
