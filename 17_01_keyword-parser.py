# Parse Web of Science XML file
#
# - collect keywords
#
#

import sys
import os
import io
import gzip
from lxml import etree
from multiprocessing import Pool
from util.util import cout, with_stacktrace

WOK = 'http://scientific.thomsonreuters.com/schema/wok5.4/public/FullRecord'
USAGE = '%s <path>' % sys.argv[0]
namespaces = {'w': WOK}
PROGRESS_SIZE = 100000
OUTPUT_DIR = '/data/WoS/17_01'

#class Keyword():


#
# Worker
#
@with_stacktrace
def work(path):
    counter = 0

    base, ext = os.path.splitext(path)
    out_path = os.path.join(OUTPUT_DIR, 'parsed_' + os.path.basename(base) + '.csv')

    # extractors
    rec2uid = lambda r: r.xpath('w:UID/text()', namespaces=namespaces)[0]
    rec2keywords = lambda r: r.xpath('w:static_data/w:fullrecord_metadata/w:keywords/w:keyword/text()', namespaces=namespaces)

#    rec2names = lambda r: r.xpath('w:static_data/w:summary/w:names/w:name[@role="author"]', namespaces=namespaces)

    # get iterator
    cout('Reading: %s' % path)
    with gzip.open(path, 'rb') as f_input:
        context = etree.iterparse(f_input, events=('end',), tag='{%s}REC' % WOK)

        cout('Writing: %s' % out_path)
        with io.open(out_path, 'w') as f_keywords:
            counter = 0
            for event, record in context:
                if counter % PROGRESS_SIZE == 0:
                    cout('Processing: %d' % counter)

                uid= rec2uid(record)

                try:
                    keywords = rec2keywords(record)

                    for x in keywords:
                        f_keywords.write(x + "\n")

                except Exception as e:
                    print('Error: %s' % uid)
                    raise

                record.clear()
                counter += 1

        cout('Finished: count=%d' % counter)
    return counter

#
# Main function
#
def main():
    # parse args
    if len(sys.argv) < 2:
        print(USAGE)
        sys.exit(1)

    # create directory
    if not os.path.exists(OUTPUT_DIR):
        print('Creating directory: %s' % OUTPUT_DIR)
        os.mkdir(OUTPUT_DIR)

    pool = Pool()
    p = pool.map_async(work, sys.argv[1:])
    p.get(86400)
    return 0


if __name__ == '__main__':
    main()
