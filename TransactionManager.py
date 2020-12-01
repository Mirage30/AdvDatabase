import re
from DataManager import DataManager


class Parser:
    done_flag = False

    def translate(self, line):
        """
        Translate an input line (str) into command
        """ 
        if self.done_flag:
            return None
        line = line.split('//')[0].strip()
        if line:
            if line.startswith("==="):
                self.done_flag = True
                return None
            return re.findall(r"[\w]+", line)


class InvalidInputError(Exception):
    def __init__(self, message):
        self.message = message


class Transaction:
    def __init__(self, trans_id, timestamp, read_only):
        """
        trans_id (str)
        timestamp (int)
        read_only (bool)
        """
        self.trans_id = trans_id
        self.timestamp = timestamp
        self.read_only = read_only


class Operation:
    def __init__(self, operation_type, trans_id, var_id, value=None):
        """
        operation_type (str): "R" or "W"
        trans_id (str)
        var_id (str)
        value (int)
        """
        self.operation_type = operation_type
        self.trans_id = trans_id
        self.var_id = var_id
        self.value = value


class TransactionManager:
    parser = Parser()

    def __init__(self):
        self.transaction_table = {}
        self.timestamp = 0
        self.operation_queue = [] # queue of Operation
        self.data_manager_list = [] # list of DataManager
        for i in range(1, 11):
            self.data_manager_list.append(DataManager(i))


    def get_command(self, line):
        """
        process a line of command
        line: A single line of command
        """
        args = self.parser.translate(line) # command and args or None
        if args:
            print("raw line : " + line.strip())

            command = args.pop(0)
            if command == "begin":
                self.begin(args[0], False)
            elif command == "beginRO":
                self.begin(args[0], True)
            elif command == "R":
                self.add_read(args[0], args[1])
            elif command == "W":
                self.add_write(args[0], args[1], args[2])
            elif command == "dump":
                self.dump()
            elif command == "end":
                self.end(args[0])
            elif command == "fail":
                self.fail(int(args[0]))
            elif command == "recover":
                self.recover(int(args[0]))
            else:
                raise InvalidInputError("ERROR: Invalid Input: {}".format(command))

            self.timestamp += 1
            self.execute()
            

    def execute(self):
        """
        Go through the operation queue, execute those could be run
        """
        for ope in list(self.operation_queue):
            if not self.transaction_table.get(ope.trans_id):
                self.operation_queue.remove(ope)
            else:
                res = False
                if ope.operation_type == 'R':
                    res = self.read(ope)
                elif ope.operation_type == 'W':
                    res = self.write(ope)
                if res:
                    self.operation_queue.remove(ope)


    def add_read(self, trans_id, var_id):
        """ 
        Add a read operation to operation queue
        trans_id (str)
        var_id (str)
        """
        if not self.transaction_table.get(trans_id):
            raise InvalidInputError("ERROR: Transaction {} does not exist".format(trans_id))
        self.operation_queue.append(Operation("R", trans_id, var_id))


    def add_write(self, trans_id, var_id, value):
        """ 
        Add a write operation to operation queue
        trans_id (str)
        var_id (str)
        value (str)
        """
        if not self.transaction_table.get(trans_id):
            raise InvalidInputError("ERROR: Transaction {} does not exist".format(trans_id))
        self.operation_queue.append(Operation("W", trans_id, var_id, int(value)))


    def begin(self, trans_id, read_only):
        """ 
        Begin a transaction with id trans_id
        trans_id (str)
        read_only (bool)
        """
        if self.transaction_table.get(trans_id):
            raise InvalidInputError("ERROR: Transaction {} already exists".format(trans_id))
        self.transaction_table[trans_id] = Transaction(trans_id, self.timestamp, read_only)
        if not read_only:
            print("Transaction {} begins".format(trans_id))
        else:
            print("Read-only transaction {} begins".format(trans_id))


    def read(self, operation : Operation):
        """ 
        Read a variable
        operation (Operation)
        Return : (bool) whether read successfully
        """
        if not self.transaction_table.get(operation.trans_id):
            raise InvalidInputError("ERROR: Transaction {} does not exist".format(operation.trans_id))
        for dm in self.data_manager_list:
            res, val = False, 0
            if self.transaction_table[operation.trans_id].read_only:
                res, val = dm.read_snapshot(self.transaction_table[operation.trans_id].timestamp, operation.var_id)
            else:
                res, val = dm.read(operation.trans_id, operation.var_id)
            if res:
                print("Transaction {} read from site {} ==> Result: {}: {}".format(operation.trans_id, dm.site_id, operation.var_id, val))
                return True
        return False


    def write(self, operation : Operation):
        """ 
        Write a variable
        operation (Operation)
        Return : (bool) whether write successfully
        """
        if not self.transaction_table.get(operation.trans_id):
            raise InvalidInputError("ERROR: Transaction {} does not exist".format(operation.trans_id))
        for dm in self.data_manager_list:
            if not dm.check_write(operation.trans_id, operation.var_id):
                return False
        
        for dm in self.data_manager_list:
            dm.write(operation.trans_id, operation.var_id, operation.value)
        print("Transaction {} write value {} to {}".format(operation.trans_id, operation.value, operation.var_id))
        return True


    def dump(self):
        """ 
        begin a transaction with id trans_id
        """
        print("dump")


    def end(self, trans_id):
        """ 
        begin a transaction with id trans_id
        """
        print("end")


    def fail(self, site_id):
        """ 
        begin a transaction with id trans_id
        """
        print("fail")


    def recover(self, site_id):
        """ 
        begin a transaction with id trans_id
        """
        print("recover")

