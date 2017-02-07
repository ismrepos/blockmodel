# Parse Web of Science XML file
#
# - collect keywords
#
#

import sys
USAGE = '%s <path>' % sys.argv[0]

def main():
    # parse args
    if len(sys.argv) < 2:
        print(USAGE)
        sys.exit(1)


if __name__ == '__main__':
    main()
