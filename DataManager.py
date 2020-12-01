class LockManager:
    def __init__(self):


class Variable:
    def __init__(self, var_id, value):
        """
        var_id (int)
        """
        self.var_id = var_id
        self.commit_val = value
        self.lock_manager = LockManager()


class DataManager:
    def __init__(self, site_id):
        """
        site_id (int)
        variable_table (dict -- (str) x1 : Variable(x1))
        """
        self.site_id = site_id
        self.variable_table = {}

        for i in range(1, 21):
            var_idx = "x" + str(i)
            if i % 2 == 0:
                self.variable_table[var_idx] = Variable()

    
    def read(self, trans_id, var_id):
        """
        read a value
        """
        if self.variable_table.get(var_id):
            return False 


        print("read in DM")
        return True

    
    def write(self, trans_id, var_id, value):
        """
        write a value
        """
        print("write in DM")
        return True