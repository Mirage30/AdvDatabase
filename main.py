import sys
from TransactionManager import TransactionManager


if __name__ == '__main__':
    trans_manager = TransactionManager()

    if len(sys.argv) >= 2:
        filename = sys.argv[1]
        print("Reading input from file: {} ...".format(filename))
        try:
            with open(filename, 'r') as f:
                lines = f.readlines()
                for line in lines:
                    # print(line)
                    trans_manager.get_command(line)
        except IOError:
            print("ERROR: Cannot open file {}".format(filename))
    else:
        print("Reading input from standard input...")
        print('You can quit the program by typing "QUIT"...')
        while True:
            line = input()
            if line.strip() == 'QUIT':
                print("Exiting...")
                break
            trans_manager.get_command(line)
            # print(line)


# class test:
#     def __init__(self, x):
#         self.x = x

