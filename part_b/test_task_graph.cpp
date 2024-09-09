#include <cstdio>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <mutex>

class TaskInfo {
    public:
        int task_id;
};

class TaskGraph {
    public:
        void addDeps(const std::vector<int>& deps, TaskInfo task_info);
        std::vector<TaskInfo> delDep(int task_id);
        void dump();
    private:
        std::mutex mtx;
        std::unordered_map<int, TaskInfo> task_info_store;
        std::unordered_map<int, std::unordered_set<int>> indegree;
        std::unordered_map<int, std::unordered_set<int>> outdegree;
};

void TaskGraph::addDeps(const std::vector<int>& deps, TaskInfo task_info) {
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

std::vector<TaskInfo> TaskGraph::delDep(int task_id) {
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
    printf("indegree:\n");
    for (auto it : indegree) {
        printf("%d: ", it.first);
        for (auto i : it.second) {
            printf("%d ", i);
        }
        printf("\n");
    }
    printf("outdegree:\n");
    for (auto it : outdegree) {
        printf("%d: ", it.first);
        for (auto i : it.second) {
            printf("%d ", i);
        }
        printf("\n");
    }
}

int main(void) {
    TaskGraph task_graph;
    std::vector<int> noDeps = {};
    TaskInfo task_info0;
    TaskInfo task_info1;
    TaskInfo task_info2;
    TaskInfo task_info3;
    TaskInfo task_info4;
    TaskInfo task_info5;
    task_info0.task_id = 0;
    task_info1.task_id = 1;
    task_info2.task_id = 2;
    task_info3.task_id = 3;
    task_info4.task_id = 4;
    task_info5.task_id = 5;
    task_graph.addDeps(noDeps, task_info0);
    std::vector<int> deps0 = {0};
    std::vector<int> deps1 = {1, 2};
    std::vector<int> deps2 = {3, 4};

    task_graph.addDeps(deps0, task_info1);
    task_graph.addDeps(deps0, task_info2);
    task_graph.addDeps(deps1, task_info3);
    task_graph.addDeps(deps1, task_info4);
    task_graph.addDeps(deps2, task_info5);
    std::vector<TaskInfo> v;
    std::vector<TaskInfo> v1;
    std::vector<TaskInfo> v2;

    task_graph.dump();
    v = task_graph.delDep(0);
    for (auto a : v) {
        printf("%d ", a.task_id);
    }
    printf("\n");
    v1 = task_graph.delDep(1);
    for (auto a : v1) {
        printf("%d ", a.task_id);
    }
    printf("\n");
    v2 = task_graph.delDep(2);
    for (auto a : v2) {
        printf("%d ", a.task_id);
    }
    printf("\n");
    task_graph.dump();
    task_graph.delDep(3);
    task_graph.delDep(4);
    task_graph.dump();
    task_graph.delDep(5);
    task_graph.dump();
    return 0;
}
