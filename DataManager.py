from collections import defaultdict

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

        graph = defaultdict(set)
        for var_idx, var in self.variable_table.items():
            lm = var.lock_manager
            if not lm.current_lock or not lm.lock_queue:
                continue
            for lock_in_queue in lm.queue:
                cur_lock = lm.current_lock
                if cur_blocks(cur_lock, lock_in_queue):
                    if cur_lock.lock_type == "R":
                        for trans_id in cur_lock.share_list:
                            if trans_id != lock_in_queue.trans_id:
                                graph[lock_in_queue.trans_id].add(trans_id)



