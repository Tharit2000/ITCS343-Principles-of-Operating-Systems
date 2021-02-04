#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <sched.h>
#include <string.h>

#include "common.h"
#include "common_threads.h"

#ifdef linux
#include <semaphore.h>
#elif __APPLE__
#include "zemaphore.h"
#endif

#define MAX_BUFFER 500
#define MAX_WAIT 80
#define MAX_BATCH 8
#define OVERHEAD_MS 160
#define PER_INPUT_MS 4

struct request_job_t 
{
    int input_id;
    double request_ms;
    double arrival_ms;
};

struct batch_job_t 
{
    struct request_job_t inputs[MAX_BATCH];
    int num_inputs;
};

FILE *out_fp;

double start_time;
struct request_job_t input_buffer[MAX_BUFFER];
struct batch_job_t batch_buffer[MAX_BUFFER];

int input_buffer_r_pointer = 0;
int input_buffer_w_pointer = 0;

int batch_buffer_w_pointer = 0;
int batch_buffer_r_pointer = 0;
int num_input_requested = 0;
int num_input_responded = 0;

char batcher_str[MAX_BATCH * 10];
char worker1_str[MAX_BATCH * 10];
char worker2_str[MAX_BATCH * 10];

pthread_mutex_t mutex;
sem_t workerSignal;
int request_counter = 0;
int temp_request_counter = 0;
int time_temp = 0;
double turnaround_time = 0;
int arrival_time[MAX_BUFFER];

void write_batch(struct batch_job_t *batch, char *output) 
{
    int bi = 0;
    int si = 0;
    if (batch)
    {
        for (bi = 0; bi < batch->num_inputs; bi++) 
        {
            si += sprintf(&output[si], "%d", batch->inputs[bi].input_id);
            si += sprintf(&output[si], "%s", " ");
        }
    }
    else
    {
        si += sprintf(&output[si], "%s", "");
    }
    
}

void log_output(char *event) 
{
    double cur_time = GetMSTime() - start_time;
    fprintf(
        out_fp,
        "%lf: %s | %s| %s| %s\n",
        cur_time, event, batcher_str, worker1_str, worker2_str);
}

void *client(void *arg) 
{
    char *input_file = arg;
    FILE* in_fp = fopen(input_file, "r");
    int next_ms = 0;
    int input_id = -1;
    double request_time = -1;
    struct request_job_t *new_job;
    
    do 
    {
        pthread_mutex_lock(&mutex); // lock a mutex
        
        printf("client: %d\n", input_buffer_w_pointer);
        new_job = &input_buffer[input_buffer_w_pointer];
        input_buffer_w_pointer++;


        fscanf (in_fp, "%d%d", &input_id, &next_ms);
        request_time = GetMSTime() - start_time;
        new_job->request_ms = request_time;
        new_job->input_id = input_id;
        pthread_mutex_unlock(&mutex); // unlock a mutex

        if (next_ms < 0)
        {
            arrival_time[request_counter] = time_temp;
            printf("ID %d arrive at %d ms\n", request_counter, arrival_time[request_counter]);
            break;
        }
        else
        {
            arrival_time[request_counter] = time_temp;
            printf("ID %d arrive at %d ms\n", request_counter, arrival_time[request_counter]);
            time_temp += next_ms;
            request_counter++; // total number of requests +1
        }
        if (next_ms > 0) 
        {
            msleep(next_ms);
        }
        
    } while (!feof (in_fp));
    
    fclose(in_fp);
    printf("client exits\n");
    return NULL;
}

void *batcher(void *arg) 
{
    int *program_over = (int *)arg;
    int append_mode = 0;
    struct request_job_t *new_job;
    struct request_job_t *job;
    struct batch_job_t *cur_batch;
    int new_job_received = 0;
    double next_batch_at_ms = 1e9;

    cur_batch = &batch_buffer[batch_buffer_w_pointer];
    cur_batch->num_inputs = 0;
    while (1) 
    {
        pthread_mutex_lock(&mutex);

        if (input_buffer_r_pointer < input_buffer_w_pointer) 
        {

            printf("batcher: %d\n", input_buffer_r_pointer);
            new_job = &input_buffer[input_buffer_r_pointer];
            new_job->arrival_ms = GetMSTime() - start_time;
            input_buffer_r_pointer++;
            pthread_mutex_unlock(&mutex);
            num_input_requested++;
            new_job_received = 1;
        }
        else
        {
            pthread_mutex_unlock(&mutex);
        }
        
        if (append_mode == 1 && 
            batch_buffer_r_pointer > batch_buffer_w_pointer) 
        {
            batch_buffer_w_pointer++;
            cur_batch = &batch_buffer[batch_buffer_w_pointer];
            cur_batch->num_inputs = 0;
            next_batch_at_ms = 1e9;
            append_mode = 0;
        }
        if (new_job_received == 1) 
        {
            if (next_batch_at_ms > new_job->arrival_ms + MAX_WAIT) 
            {
                next_batch_at_ms = new_job->arrival_ms + MAX_WAIT;
            }
            job = &cur_batch->inputs[cur_batch->num_inputs];
            job->input_id = new_job->input_id;
            job->arrival_ms = new_job->arrival_ms;
            cur_batch->num_inputs++;
            write_batch(cur_batch, batcher_str);
            log_output("INPUT");
        }
        if (cur_batch->num_inputs == MAX_BATCH || 
            (GetMSTime() - start_time) > next_batch_at_ms ) 
        {
            if (cur_batch->num_inputs < MAX_BATCH) 
            {
                if (append_mode == 0) 
                {
                    write_batch(cur_batch, batcher_str);
                    log_output("QUEUE");
                    sem_post(&workerSignal);
                }
                append_mode = 1;
            } 
            else 
            {
                batch_buffer_w_pointer++;
                cur_batch = &batch_buffer[batch_buffer_w_pointer];
                cur_batch->num_inputs = 0;
                next_batch_at_ms = 1e9;
                append_mode = 0;
                write_batch(cur_batch, batcher_str);
                log_output("QUEUE");
                write_batch(NULL, batcher_str);
                sem_post(&workerSignal);
            }
        }
        
        if (*program_over == -1 && cur_batch->num_inputs == 0) 
        {
            break;
        }
        
        new_job_received = 0;
        msleep(1);
    }
    printf("batcher exits\n");
    return NULL;
}

void *worker(void *arg) 
{
    int *program_over = (int *)arg;
    int worker_id = *(int *)arg;
    struct batch_job_t *cur_batch;
    double work_ms = -1;
    while (1) 
    {
        if (*program_over == -1 && num_input_requested == num_input_responded) 
        {
            break;
        }
        printf("worker %d: %d\n",worker_id, batch_buffer_r_pointer);

        sem_wait(&workerSignal);

        cur_batch = &batch_buffer[batch_buffer_r_pointer];
        batch_buffer_r_pointer++;
        num_input_responded += cur_batch->num_inputs;
        if (worker_id == 1) 
        {
            write_batch(cur_batch, worker1_str);
        } 
        else 
        {
            write_batch(cur_batch, worker2_str);
        }
        if (cur_batch->num_inputs < MAX_BATCH)
        {
            write_batch(NULL, batcher_str);
        }
        log_output("START");
        // pretending that I am working, but actually sleeping.
        // isn't that great?
        work_ms = (long) cur_batch->num_inputs * PER_INPUT_MS + OVERHEAD_MS;
        msleep(work_ms);
        printf("worker %d: DONE!\n", worker_id);

        // calculate turnaround time
        double cur_time = GetMSTime() - start_time;
        for (int i = 0; i < cur_batch->num_inputs; i++)
        {
            turnaround_time += cur_time - arrival_time[temp_request_counter];
            temp_request_counter++;
        }

        log_output("DONE");
        if (worker_id == 1)
        {
            write_batch(NULL, worker1_str);
        }
        else
        {
            write_batch(NULL, worker2_str);
        }
    }
    printf("worker %d exits\n", worker_id);
    return NULL;
}

int main(int argc, char *argv[]) 
{
    if (argc != 3) 
    {
        // Invalid arguments
        printf("Invalid arguments!\n");
        printf("Usage: ./batch_buffer <path/input_file> <path/output_file>");
        return 1;
    }
    out_fp = fopen(argv[2], "w");

    int program_over = 0;
    int worker1 = 1;
    int worker2 = 2;
    int rc;
    arrival_time[0] = 0;
    struct sched_param param;
    pthread_attr_t tattr;
    pthread_t client_thread, batcher_thread, worker1_thread, worker2_thread;
    
    pthread_mutex_init(&mutex, NULL);
    sem_init(&workerSignal, 0, 0);
    
    rc = pthread_attr_init (&tattr);
    rc = pthread_attr_getschedparam (&tattr, &param);
    param.sched_priority  = 20;
    rc = pthread_attr_setschedparam (&tattr, &param);
    start_time = GetMSTime();
    Pthread_create(&client_thread, &tattr, client, argv[1]);

    // START HERE: Create threads.

    Pthread_create(&batcher_thread, &tattr, batcher, &program_over);
    Pthread_create(&worker1_thread, &tattr, worker, &worker1);
    Pthread_create(&worker2_thread, &tattr, worker, &worker2);
    Pthread_join(client_thread, NULL);
    program_over = -1;
    Pthread_join(batcher_thread, NULL);
    worker1 = -1;
    worker2 = -1;
    Pthread_join(worker1_thread, NULL);
    Pthread_join(worker2_thread, NULL);

    fprintf(out_fp, "\nAverage Turnaround Time : %lf ms", turnaround_time / (request_counter+1));
    fclose(out_fp);
    return 0;
}
