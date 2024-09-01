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
        mutex_.lock();
        if (stop_) {
            mutex_.unlock();
            break;
        }
        if (cur_task_ >= num_total_tasks_ && cur_task_ != 0) {
            work_to_do_ = false;
            if (++finished_ == numThreads()) {
                mutex_.unlock();
                barrier_.notify_all();
                continue;
            }
        }
        if (!work_to_do_) {
            mutex_.unlock();
            continue;
        }
        int now = cur_task_++;
        mutex_.unlock();
        runnable_->runTask(now, num_total_tasks_);
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
    mutex_.lock();
    stop_ = true;
    mutex_.unlock();
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
    std::unique_lock<std::mutex> lk(mutex_);
    num_total_tasks_ = num_total_tasks;
    runnable_ = runnable;
    work_to_do_ = true;
    barrier_.wait(lk);
    finished_ = 0;
    cur_task_ = 0;
    lk.unlock();
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
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

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    std::unique_lock<std::mutex> lk(mutex_);
    stop_ = true;
    lk.unlock();
    cond_.notify_all();
    for (int i = 0; i < numThreads(); ++i) {
        thread_pool_[i].join();
    }
    delete[] thread_pool_;
}

void TaskSystemParallelThreadPoolSleeping::pullTasks() {
    std::unique_lock<std::mutex> lk{mutex_, std::defer_lock};
    while (true) {
        lk.lock();
        if (stop_) {
            lk.unlock();
            break;
        }
        if (cur_task_ >= num_total_tasks_ && cur_task_ != 0) {
            work_to_do_ = false;
            if (++finished_ == numThreads()) {
                lk.unlock();
                // printf("==== notify ====\n");
                barrier_.notify_all();
                continue;
            }
        }
        if (!work_to_do_) {
            // printf("[%d] sleep\n", std::this_thread::get_id());
            cond_.wait(lk);
        }
        if (stop_) {
            lk.unlock();
            break;
        }
        int now = cur_task_++;
        lk.unlock();
        runnable_->runTask(now, num_total_tasks_);
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::unique_lock<std::mutex> lk(mutex_);
    num_total_tasks_ = num_total_tasks;
    runnable_ = runnable;
    work_to_do_ = true;
    cond_.notify_all();
    barrier_.wait(lk);
    // printf("caller thread wakes up\n");
    finished_ = 0;
    cur_task_ = 0;
    num_total_tasks_ = 0;
    runnable_ = nullptr;
    lk.unlock();
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
