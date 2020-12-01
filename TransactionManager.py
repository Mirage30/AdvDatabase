import re


class Parser:
    done_flag = False

    def translate(self, line):
        """
        Translate an input line (str) into command
        """ 
        if done_flag:
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
        self.operation_queue = []


    def get_command(self, line):
        """
        process a line of command
        line: A single line of command
        """
        args = self.parser.translate(line)
        if args:
            command = args.pop(0)
            if command == "begin":
                begin(args[0], False)
            elif command == "beginRO":
                begin(args[0], True)
            elif command == "R":
                add_read(args[0], args[1])
            elif command == "W":
                add_write(args[0], args[1], args[2])
            elif command == "dump":
                dump()
            elif command == "end":
                end(args[0])
            elif command == "fail":
                fail(int(args[0]))
            elif command == "recover":
                recover(int(args[0]))
            else:
                raise InvalidInputError("ERROR: Invalid Input: {}".format(command))


    def add_read(self, trans_id, var_id):
        """ 
        Add a read operation to operation queue
        """
        self.operation_queue.append(Operation("R", trans_id, var_id))


    def add_write(self, trans_id, var_id, value):
        """ 
        Add a write operation to operation queue
        """
        self.operation_queue.append(Operation("W", trans_id, var_id, value))


    def begin(self, trans_id, read_only):
        """ 
        begin a transaction with id trans_id
        trans_id (str)
        read_only (bool)
        """
        if self.transaction_table.get(trans_id):
            raise InvalidInputError("ERROR: The transaction {} already exists".format(trans_id))
        self.transaction_table[trans_id] = Transaction(trans_id, self.timestamp, read_only)
        if not read_only:
            print("Transaction {} begins".format(trans_id))
        else:
            print("Read-only transaction {} begins".format(trans_id))


    def dump(self):
        """ 
        begin a transaction with id trans_id
        """


    def end(self, trans_id):
        """ 
        begin a transaction with id trans_id
        """


    def fail(self, site_id):
        """ 
        begin a transaction with id trans_id
        """


    def recover(self, site_id):
        """ 
        begin a transaction with id trans_id
        """

