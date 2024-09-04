#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {
    num_threads_ = num_threads;
}
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
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    thread_pool_ = new std::thread[num_threads];
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn()  { delete[] thread_pool_; }

void TaskSystemParallelSpawn::threadRun(IRunnable* runnable, int task_id, int num_total_tasks) {
    int now = -1;
    while (now < num_total_tasks) {
        mutex_.lock();
        now = cur_task_++;
        mutex_.unlock();
        if (now >= num_total_tasks) {
            break;
        }
        runnable->runTask(now, num_total_tasks);
    }
}

void TaskSystemParallelSpawn::threadRunStatic(IRunnable* runnable, int task_id, int num_total_tasks) {
    for (int i = task_id; i < num_total_tasks; i += numThreads()) {
        runnable->runTask(i, num_total_tasks);
    }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
   
   
   for (int i = 0; i < numThreads(); ++i) {
       thread_pool_[i] = std::thread([=]() { threadRun(runnable, i, num_total_tasks); });
   }

    for (int i = 0; i < numThreads(); i++) {
        thread_pool_[i].join();
    }
    
    cur_task_ = 0;
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
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

void TaskSystemParallelThreadPoolSpinning::pullTasks() {
    while (true) {
        if (stop_) {
            break;
        }
        int now;
        {
            std::unique_lock<std::mutex> lk(mutex_);
            if (cur_task_ >= num_total_tasks_) {
                continue;
            }
            now = cur_task_++;
        }
        runnable_->runTask(now, num_total_tasks_);
        finished_++;
    }
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    thread_pool_ = new std::thread[num_threads]; 
    for (int i = 0; i < num_threads; i++) {
        thread_pool_[i] = std::thread([=]() { pullTasks(); });
    }
 }

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() { 
    stop_ = true;
    for (int i = 0; i < numThreads(); ++i) {
        thread_pool_[i].join();
    }
    delete[] thread_pool_; 
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    mutex_.lock();
    num_total_tasks_ = num_total_tasks;
    runnable_ = runnable;
    mutex_.unlock();
    while (finished_ < num_total_tasks_) {
        ;
    }
    finished_ = 0;
    cur_task_ = 0;
    num_total_tasks_ = 0;
    runnable_ = nullptr;
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(
    int num_threads) : ITaskSystem(num_threads) {

    done = false;
    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([this]() { this->worker(); });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    {
        std::unique_lock<std::mutex> lock(mtx);
        done = true;
    }
    producer_cv.notify_all();
    for (auto& thread : threads) {
        thread.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::worker() {
    while (true) {
        int task_id;
        {
            std::unique_lock<std::mutex> lock(mtx);
            producer_cv.wait(lock, [this]() { return current_task < num_total_tasks || done; });
            if (done) {
                break;
            }
            task_id = current_task++;
        }
        runnable->runTask(task_id, num_total_tasks);
        std::unique_lock<std::mutex> lock(mtx);
        num_tasks_completed++;
        if (num_tasks_completed == num_total_tasks) {
            consumer_cv.notify_all();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    {
        std::unique_lock<std::mutex> lock(mtx);
        current_task = 0;
        num_tasks_completed = 0;
        this->num_total_tasks = num_total_tasks;
        this->runnable = runnable;
    }
    producer_cv.notify_all();
    std::unique_lock<std::mutex> lock(mtx);
    consumer_cv.wait(lock, [this]() { return num_tasks_completed == this->num_total_tasks; });
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
