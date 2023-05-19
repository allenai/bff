import json
import sys


def print_colored(text, color, end='\n'):
    color_codes = {
        'red': '\033[31m',
        'green': '\033[32m',
        'yellow': '\033[33m',
        'blue': '\033[34m',
        'magenta': '\033[35m',
        'cyan': '\033[36m',
        'white': '\033[37m',
        'reset': '\033[0m',
    }
    print(f"{color_codes[color]}{text}{color_codes['reset']}", end=end)

def main():
    # read lines from stdin
    for line in sys.stdin:
        doc = json.loads(line)
        text = doc['text'].encode('utf-8')
        first_end = int(doc['bff_duplicate_spans'][0][0]) if doc['bff_duplicate_spans'] else len(text)
        print(doc['id'])
        print(doc['source'])
        print(text[0:first_end].decode('utf-8'), end='')
        for i in range(len(doc['bff_duplicate_spans'])):
            cur_span = doc['bff_duplicate_spans'][i]
            next_start = doc['bff_duplicate_spans'][i+1][0] if i+1 < len(doc['bff_duplicate_spans']) else len(text)
            print_colored(text[int(cur_span[0]):int(cur_span[1])].decode('utf-8'), 'red', end='')
            print(text[int(cur_span[1]):int(next_start)].decode('utf-8'), end='')
        print('\n------------------#################------------------')

if __name__ == "__main__":
    main()
