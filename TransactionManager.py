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


class TransactionManager:
    parser = Parser()
    def __init__(self):
        self.transaction_table = {}
        self.timestamp = 0

    def get_command(self, line):
        """
        process a line of command
        line: A single line of command
        """
        
