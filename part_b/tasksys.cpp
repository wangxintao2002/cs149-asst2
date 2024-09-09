#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */



const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

void TaskGraph::addDeps(const std::vector<TaskID>& deps, TaskInfo task_info) {
    std::unique_lock<std::mutex> lk(mtx);
    if (deps.size() == 0) {
        return;
    }

    for (auto task : deps) {
        outdegree[task].insert(task_info.task_id);
        indegree[task_info.task_id].insert(task);
    }
    task_info_store[task_info.task_id] = task_info;
}

std::vector<TaskInfo> TaskGraph::delDep(TaskID task_id) {
    std::unique_lock<std::mutex> lk(mtx);
    if (outdegree[task_id].size() == 0) {
        return {};
    }
    std::vector<TaskInfo> tasks;
    auto it = outdegree[task_id].begin();
    while (it != outdegree[task_id].end()) {
        int id = *it;
        indegree[id].erase(task_id);
        if (indegree[id].size() == 0) {
            tasks.push_back(task_info_store[id]);
            task_info_store.erase(id);
        }
        it = outdegree[task_id].erase(it);
    }
    return tasks;
}

void TaskGraph::dump() {
    // printf("indegree:\n");
    for (auto it : indegree) {
        // printf("%d: ", it.first);
        for (auto i : it.second) {
            // printf("%d ", i);
        }
        // printf("\n");
    }
    // printf("outdegree:\n");
    for (auto it : outdegree) {
        // printf("%d: ", it.first);
        for (auto i : it.second) {
            // printf("%d ", i);
        }
        // printf("\n");
    }
}

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    done = false;
    task_graph = new TaskGraph();
    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([this]() { this->worker(); });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    {
        std::unique_lock<std::mutex> lock(mtx);
        done = true;
    }
    producer_cv.notify_all();
    for (auto& thread : threads) {
        thread.join();
    }
    delete task_graph;
}

void TaskSystemParallelThreadPoolSleeping::worker() {
    while (true) {
        TaskInfo task_info;
        {
            std::unique_lock<std::mutex> lock(mtx);
            if (do_sync && working_tasks.empty() && ready_queue.empty()) {
                sync_cv.notify_one();
            }
            producer_cv.wait(lock, [this]() { return !ready_queue.empty() || done || !working_tasks.empty(); });
            if (done) {
                break;
            }
            if (working_tasks.empty()) {
                task_info = ready_queue.front();
                ready_queue.pop();
                working_tasks[task_info.task_id] = task_info;
            } else {
                auto it = working_tasks.begin();
                while (it != working_tasks.end()) {
                    it->second.current_task++;
                    task_info = it->second;
                    if (task_info.current_task < task_info.num_total_tasks) {
                        break; 
                    }
                    ++it;
                }
                if (it == working_tasks.end()) {
                    continue;
                }
            }
        } 
        // printf("%d run %d: %d\n", std::this_thread::get_id(), task_info.task_id, task_info.current_task);
        assert(task_info.current_task < task_info.num_total_tasks);
        task_info.runnable->runTask(task_info.current_task, task_info.num_total_tasks);
        // printf("%d finish %d: %d\n", std::this_thread::get_id(), task_info.task_id, task_info.current_task);
        std::unique_lock<std::mutex> lk(mtx);
        working_tasks[task_info.task_id].num_task_completed++;
        if (working_tasks[task_info.task_id].num_task_completed == working_tasks[task_info.task_id].num_total_tasks) {
            // printf("%d fully finish %d pop queue %d\n", std::this_thread::get_id(), task_info.task_id, ready_queue.size());
            working_tasks.erase(task_info.task_id);
            // update DAG
            auto runnable_tasks = task_graph->delDep(task_info.task_id);
            for (auto task : runnable_tasks) {
                ready_queue.push(task);
                // printf("%d ready in queue\n", task.task_id);
            }
            producer_cv.notify_all();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //
    TaskInfo task_info;
    task_info.runnable = runnable;
    task_info.current_task = 0;
    task_info.num_total_tasks = num_total_tasks;
    task_info.num_task_completed = 0;
    task_info.task_id = task_id_global;

    if (deps.empty()) {
        std::unique_lock<std::mutex> lk(mtx);
        // printf("%d ready in queue\n", task_info.task_id);
        ready_queue.push(task_info);
    }
    task_graph->addDeps(deps, task_info);
    if (!deps.empty()) {
        // printf("%d wait\n", task_info.task_id);
        waiting++;
    }
    return task_id_global++;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    std::unique_lock<std::mutex> lk(mtx);
    do_sync = true;
    producer_cv.notify_all();
    sync_cv.wait(lk, [this] { return working_tasks.empty() && ready_queue.empty(); });
    // printf("sync wake up\n");
    do_sync = false;
}
