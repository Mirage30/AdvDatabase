from collections import defaultdict
from ErrorHandler import InvalidInputError

class LockItem:
    def __init__(self, var_id, lock_type, trans_id):
        """
        var_id (str)
        lock_type (str) : "W" or "R"
        """
        self.var_id = var_id
        self.lock_type = lock_type
        self.trans_id = trans_id


class ReadLockItem(LockItem):
    def __init__(self, var_id, lock_type, trans_id):
        super().__init__(var_id, lock_type, trans_id)
        self.share_list = {trans_id}


class WriteLockItem(LockItem):
    def __init__(self, var_id, lock_type, trans_id):
        super().__init__(var_id, lock_type, trans_id)


class LockManager:
    def __init__(self):
        """
        current_lock (LockItem)
        lock_queue (list) : transactions that are waiting for lock
        """
        self.current_lock = None
        self.lock_queue = []


    def is_writelock_waiting(self, trans_id = None):
        """
        check whether there are any writelock waiting in the queue
        Return (bool)
        """
        for lk in self.lock_queue:
            if lk.lock_type == "W" and lk.trans_id != trans_id:
                return True
        return False


    def change_current_lock(self, lock: LockItem):
        """
        change the current lock
        """
        self.current_lock = lock


    def share_lock(self, trans_id):
        """
        add trans_id into the share list
        trans_id (str)
        """
        self.current_lock.share_list.add(trans_id)


    def add_lock_to_queue(self, lock: LockItem):
        """
        add a lock to the queue
        """
        for lk in self.lock_queue:
            if lk.lock_type == lock.lock_type and lk.trans_id == lock.trans_id:
                return
        self.lock_queue.append(lock)


    def promote_current_lock(self, lock: LockItem):
        """
        promote current R lock to W lock
        """
        assert(self.current_lock.lock_type == "R" and lock.lock_type == "W")
        assert(len(self.current_lock.share_list) == 1 and lock.trans_id in self.current_lock.share_list)
        self.current_lock = lock


    def release_current_lock(self, trans_id):
        """
        check whether trans_id hold the current lock
        if so, release it
        """
        if self.current_lock:
            if self.current_lock.lock_type == "R" and trans_id in self.current_lock.share_list:
                self.current_lock.share_list.remove(trans_id)
                if not len(self.current_lock.share_list):
                    self.current_lock = None
            elif self.current_lock.lock_type == "W" and trans_id == self.current_lock.trans_id:
                self.current_lock = None    


class CommitValue:
    def __init__(self, value, timestamp):
        """
        value (int)
        timestamp (int)        
        """
        self.value = value
        self.timestamp = timestamp


class Variable:
    def __init__(self, var_id, value, replicated):
        """
        var_id (str)
        value (int)
        commit_val (CommitValue)
        temp_val (int)
        replicated (bool)
        available (bool): a var is unavailable after recover from failure until a write commited
        """
        self.var_id = var_id
        self.commit_val = [CommitValue(value, 0)]
        self.temp_val = value
        self.replicated = replicated
        self.lock_manager = LockManager()
        self.available = True


class DataManager:
    def __init__(self, site_id):
        """
        site_id (int)
        variable_table (dict -- x1 (str) : Variable(x1))
        visited_transaction (set of trans_id): transactions who visited this site 
        """
        self.site_id = site_id
        self.variable_table = {}
        self.is_working = True
        self.visited_transaction = set()

        for i in range(1, 21):
            var_idx = "x" + str(i)
            if i % 2 == 0:
                self.variable_table[var_idx] = Variable(var_idx, 10 * i, True)
            if i % 10 + 1 == site_id:
                self.variable_table[var_idx] = Variable(var_idx, 10 * i, False)


    def read_snapshot(self, timestamp: int, var_id):
        """
        read a value from snapshot, the read-only transaction began at "timestamp"
        timestamp (int)
        var_id (str)
        Return: bool, int
        """
        if not self.variable_table.get(var_id):
            return False, None
        var: Variable = self.variable_table[var_id]
        res = 0
        for cval in var.commit_val:
            if cval.timestamp <= timestamp:
                res = cval.value
            else:
                break
        return True, res

    
    def read(self, trans_id, var_id):
        """
        read a value
        trans_id (str)
        var_id (str)
        Return: bool, int
        """
        if not self.variable_table.get(var_id):
            return False, None
        var: Variable = self.variable_table[var_id]        
        # if the var hasn't been visited after the site recovery
        if not var.available:
            return False, None

        if not var.lock_manager.current_lock:
            var.lock_manager.change_current_lock(ReadLockItem(var_id, "R", trans_id))
            return True, var.commit_val[-1].value
        elif var.lock_manager.current_lock.lock_type == "R":
            # transaction has R lock
            if trans_id in var.lock_manager.current_lock.share_list:
                return True, var.commit_val[-1].value

            # transaction does not have R lock
            # check write lock in lock queue 
            if var.lock_manager.is_writelock_waiting():
                var.lock_manager.add_lock_to_queue(ReadLockItem(var_id, "R", trans_id))
                return False, None
            else:
                var.lock_manager.share_lock(trans_id)
                return True, var.commit_val[-1].value
        elif var.lock_manager.current_lock.trans_id == trans_id:
            return True, var.temp_val
        
        # is being written by other transaction, add to queue
        var.lock_manager.add_lock_to_queue(ReadLockItem(var_id, "R", trans_id))
        return False, None


    def check_write(self, trans_id, var_id):
        """
        check whether a transaction can write
        Return:
        True: can write here OR var_id is not here
        False: is here but cannot write
        """
        if not self.variable_table.get(var_id):
            return True
        var: Variable = self.variable_table[var_id]
        if not var.lock_manager.current_lock:
            var.lock_manager.change_current_lock(WriteLockItem(var_id, "W", trans_id))
            return True
        elif var.lock_manager.current_lock.lock_type == "R":
            if len(var.lock_manager.current_lock.share_list) != 1 \
                    or trans_id not in var.lock_manager.current_lock.share_list \
                    or var.lock_manager.is_writelock_waiting(trans_id):
                var.lock_manager.add_lock_to_queue(WriteLockItem(var_id, "W", trans_id))
                return False
            var.lock_manager.promote_current_lock(WriteLockItem(var_id, "W", trans_id))
            return True
        else:
            if var.lock_manager.current_lock.trans_id == trans_id:
                return True
            var.lock_manager.add_lock_to_queue(WriteLockItem(var_id, "W", trans_id))
            return False

    
    def write(self, trans_id, var_id, value):
        """
        write a value
        trans_id (str)
        var_id (str)
        value (int)
        """
        if not self.variable_table.get(var_id):
            return
        self.variable_table[var_id].temp_val = value


    def dump(self):
        """
        gives the commited values of all copies of all variables at all sites
        sorted per site with all values in ascending order by variable name
        """
        updown = "UP" if self.is_working else "DOWN"
        res = "[{}]Site {} ".format(updown, self.site_id)
        for var in self.variable_table.values():
            res += "-- {}: {} ".format(var.var_id, var.commit_val[-1].value)
        print(res)


    def abort(self, trans_id):
        """
        abort a transaction
        """
        for var in self.variable_table.values():
            lm: LockManager = var.lock_manager
            lm.release_current_lock(trans_id)
            for lk in list(lm.lock_queue):
                if lk.trans_id == trans_id:
                    lm.lock_queue.remove(lk)
            # print("xxxx {}".format(len(lm.lock_queue)))


    def commit(self, trans_id, timestamp):
        """
        commit a transaction
        """
        for var in self.variable_table.values():
            lm: LockManager = var.lock_manager
            if lm.current_lock and lm.current_lock.lock_type == "W" and lm.current_lock.trans_id == trans_id:
                var.commit_val.append(CommitValue(var.temp_val, timestamp))
                var.available = True
            lm.release_current_lock(trans_id)
            
            for lk in list(lm.lock_queue):
                if lk.trans_id == trans_id:
                    # print(self.site_id)
                    # print(var.var_id)
                    raise InvalidInputError("ERROR: transaction {} commits before all operations done".format(trans_id))


    def fail(self):
        """
        fail a site, wipe out all the lock information of it
        """
        self.is_working = False
        for var in self.variable_table.values():
            var.lock_manager.current_lock = None
            var.lock_manager.lock_queue = []
            if var.replicated:
                var.available = False


    def recover(self):
        """
        recover a site
        """
        self.is_working = True
        self.visited_transaction = set()


    def generate_graph(self):

        def cur_blocks(cur_lock, lock_in_queue):
            if cur_lock == "R":
                if lock_in_queue.lock_type == "R" or \
                    (len(cur_lock.share_list) == 1 and
                    lock_in_queue.trans_id in cur_lock.share_list):
                    return False
                return True
            return lm.current_lock.trans_id != lock_in_queue.trans_id
        
        def queue_blocks(lock_j,lock_i):
            if lock_j.lock_type == "R" and lock_i.lock_type == "R":
                 return False
            return not lock_j.trans_id == lock_i.trans_id

        graph = defaultdict(set)
        for var_idx, var in self.variable_table.items():
            lm = var.lock_manager
            if not lm.current_lock or not lm.lock_queue:
                continue
            for lock_in_queue in lm.lock_queue:
                cur_lock = lm.current_lock
                if cur_blocks(cur_lock, lock_in_queue):
                    if cur_lock.lock_type == "R":
                        for trans_id in cur_lock.share_list:
                            if trans_id != lock_in_queue.trans_id:
                                graph[lock_in_queue.trans_id].add(trans_id)
                    else:
                        if cur_lock.trans_id == lock_in_queue:
                            continue
                        else:
                            graph[lock_in_queue.trans_id].add(cur_lock.trans_id)
        
            for i in range(len(lm.lock_queue)):
                lock_queue = lm.lock_queue
                for j in range(i):
                    if(queue_blocks(lock_queue[j],lock_queue[i])):
                        graph[lock_queue[i].trans_id].add(lock_queue[j].trans_id)
        return graph

            



