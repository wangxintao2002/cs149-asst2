#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <vector>
#include <cstdio>
#include <cassert>
#include <thread>
#include <unordered_set>
#include <unordered_map>
#include <atomic>
#include <queue>
#include <condition_variable>
#include <mutex>


/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

class TaskInfo {
    public:
        IRunnable* runnable;
        int current_task{0};
        int num_task_completed{0};
        int num_total_tasks{0};
        TaskID task_id;
};

class TaskGraph {
    public:
        void addDeps(const std::vector<TaskID>& deps, TaskInfo task_info);
        std::vector<TaskInfo> delDep(TaskID task_id);
        void dump();
    private:
        std::mutex mtx;
        std::unordered_map<TaskID, TaskInfo> task_info_store;
        std::unordered_map<TaskID, std::unordered_set<TaskID>> indegree;
        std::unordered_map<TaskID, std::unordered_set<TaskID>> outdegree;
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
        void worker();
    private:
        int num_threads;
        std::vector<std::thread> threads;
        std::mutex mtx;
        std::condition_variable producer_cv;
        std::condition_variable consumer_cv;
        bool done{false};
        TaskGraph* task_graph;
        bool do_sync{false};
        std::atomic<TaskID> task_id_global{0};
        std::queue<TaskInfo> ready_queue;
        std::atomic<int> waiting{0};
        std::condition_variable sync_cv;
        std::unordered_map<TaskID, TaskInfo> working_tasks;
};

#endif
